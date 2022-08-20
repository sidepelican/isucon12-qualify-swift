@preconcurrency import MySQLKit
@preconcurrency import SQLiteKit
@preconcurrency import SQLKit
import JWTKit
import TSCBasic
import Vapor

let tenantDBSchemaFilePath = "../sql/tenant/10_schema.sql"
let initializeScript = "../sql/init.sh"
let cookieName = "isuports_session"
let tenantNameRegexp = #/^[a-z][a-z0-9-]{0,61}[a-z0-9]$/#

enum Role: String {
    case admin
    case organizer
    case player
}

// 環境変数を取得する、なければデフォルト値を返す
func getEnv(key: String, defaultValue: String) -> String {
    ProcessInfo.processInfo.environment[key, default: defaultValue]
}

// 管理用DBに接続する
func connectAdminDB() -> (EventLoopConnectionPool<MySQLConnectionSource>, shutdown: () throws -> Void) {
    let configuration = MySQLConfiguration(
        hostname: getEnv(key: "ISUCON_DB_HOST", defaultValue: "127.0.0.1"),
        port: Int(getEnv(key: "ISUCON_DB_PORT", defaultValue: "3306"))!,
        username: getEnv(key: "ISUCON_DB_USER", defaultValue: "isucon"),
        password: getEnv(key: "ISUCON_DB_PASSWORD", defaultValue: "isucon"),
        database: getEnv(key: "ISUCON_DB_NAME", defaultValue: "isuports"),
        tlsConfiguration: {
            var tls = TLSConfiguration.makeClientConfiguration()
            tls.certificateVerification = .none
            return tls
        }()
    )
    
    let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    let pool = EventLoopConnectionPool(
        source: MySQLConnectionSource(configuration: configuration),
        maxConnections: 10,
        on: eventLoopGroup.next()
    )
    
    return (pool, shutdown: {
        try pool.close().wait()
        try eventLoopGroup.syncShutdownGracefully()
    })
}

// テナントDBのパスを返す
func tenantDBPath(id: Int) -> String {
    let tenantDBDir = getEnv(key: "ISUCON_TENANT_DB_DIR", defaultValue: "../tenant_db")
    return RelativePath(tenantDBDir)
        .appending(component: "\(id).db")
        .pathString
}

// 全APIにCache-Control: privateを設定する
final class CacheControlPrivateMiddleware: Middleware {
    init() {}
    func respond(to request: Request, chainingTo next: Responder) -> EventLoopFuture<Response> {
        return next.respond(to: request).map { response in
            response.headers.replaceOrAdd(name: .cacheControl, value: "private")
            return response
        }
    }
}

struct Server {
    var adminDB: any MySQLDatabase
    var threadPool: NIOThreadPool
    var eventLoopGroup: any EventLoopGroup
    
    // テナントDBに接続する
    func connectToTenantDB(id: Int) async throws -> SQLiteConnection {
        try await SQLiteConnection.open(
            storage: .file(path: tenantDBPath(id: id)),
            threadPool: threadPool,
            on: eventLoopGroup.next()
        ).get()
    }
    
    // テナントDBを新規に作成する
    func createTenantDB(id: Int) async throws {
        try await threadPool.task {
            let path = tenantDBPath(id: id)
            let result = try Process.popen(args: "sh", "-c", "sqlite3 \(path) < \(tenantDBSchemaFilePath)")
            guard result.exitStatus == .terminated(code: 0) else {
                throw StringError("failed to exec sqlite3 \(path) < \(tenantDBSchemaFilePath), out=\(try result.utf8Output()), err=\(try result.utf8stderrOutput())")
            }
        }
    }
    
    // システム全体で一意なIDを生成する
    func dispenseID(req: Request) async throws -> String {
        var lastError: Error?
        for _ in 0..<100 {
            var lastInsertID: UInt64?
            do {
                try await adminDB.logging(to: req.logger)
                    .query("REPLACE INTO id_generator (stub) VALUES (?);", ["a"], onRow: { _ in }, onMetadata: { metadata in
                        lastInsertID = metadata.lastInsertID
                    })
                    .get()
            } catch {
                if let mysqlError = error as? MySQLError,
                   case .server(let errPacket) = mysqlError,
                   errPacket.errorCode == .LOCK_DEADLOCK {
                    lastError = StringError("error REPLACE INTO id_generator: \(error)")
                    continue
                } else {
                    throw StringError("error REPLACE INTO id_generator: \(error)")
                }
            }
            
            if let lastInsertID {
                return String(lastInsertID)
            } else {
                throw StringError("error lastInsertID is nil")
            }
        }
        throw lastError ?? StringError("unexpected")
    }
    
    // run は main から呼ばれるエントリーポイントです
    func run(port: Int) async throws {
        let app = try Application(.detect(), .shared(eventLoopGroup))
        defer { app.shutdown() }
        
        app.http.server.configuration.port = port
        
        app.middleware.use(CacheControlPrivateMiddleware())
        
        app.post("/api/admin/tenants/add", use: tenantsAdd)
        
//        app.get("hello") { req in
//            return "Hello, world.\n"
//        }
//
//        app.get("version") { (req) in
//            let rows = try await adminDB.simpleQuery("SELECT version();").get()
//            return "\(rows)\n"
//        }
//
//        app.get("bind") { (req) async throws -> String in
//            let sql: SQLDatabase = adminDB.sql()
//
//            struct Row: Decodable {
//                var value: String
//            }
//            return try await sql.execute("SELECT \(bind: "binded value") as value;")
//                .first(decoding: Row.self)?
//                .value ?? "nil"
//        }
//
//        app.get("sqlite") { (req) in
//            try await createTenantDB(id: 1)
//            let db = try await connectToTenantDB(id: 1)
//            let row = try await db.sql().execute("SELECT sqlite_version();").first()!
//            return "\(row)"
//        }

        do {
            try app.start()
            try await app.running?.onStop.get()
        } catch {
            app.logger.report(error: error)
            throw error
        }
    }
    
    struct SuccessResult<T: Encodable>: Encodable {
        var status: Bool
        var data: T?
    }

    struct FailureResult: Encodable  {
        var status: Bool
        var message: String
    }

    // アクセスしてきた人の情報
    struct Viewer {
        var role: Role
        var playerID: String
        var tenantName: String
        var tenantID: Int64
    }
    
    struct Claims: JWTPayload {
        var sub: String?
        var aud: [String]?
        var role: String?
        
        func verify(using signer: JWTSigner) throws {
            // 多言語の挙動に寄せるために検査はparseViewer内で行い、CodableやJWTKitの仕組みを使用しない
        }
    }
    
    // リクエストヘッダをパースしてViewerを返す
    func parseViewer(req: Request) async throws -> Viewer {
        guard let cookie = req.cookies[cookieName] else {
            throw Abort(.unauthorized, reason: "cookie \(cookieName) is not found")
        }
        let tokenString = cookie.string
        
        let keyFilename = getEnv(key: "ISUCON_JWT_KEY_FILE", defaultValue: "../public.pem")
        let keysrc = try Data(contentsOf: URL(fileURLWithPath: keyFilename))
        
        let signer = JWTSigner.rs256(key: try .public(pem: keysrc))
        let token: Claims
        do {
            token = try signer.verify(tokenString, as: Claims.self)
        } catch {
            throw Abort(.unauthorized, reason: "error signer.verify: \(error)")
        }

        guard let tokenSub = token.sub else {
            throw Abort(.unauthorized, reason: "invalid token: subject is not found in token: \(tokenString)")
        }
        guard let tokenRole = token.role else {
            throw Abort(.unauthorized, reason: "invalid token: role is not found: \(tokenString)")
        }
        guard let role = Role(rawValue: tokenRole) else {
            throw Abort(.unauthorized, reason: "invalid token: invalid role: \(tokenString)")
        }
        // aud は1要素でテナント名がはいっている
        let tokenAud = token.aud ?? []
        guard tokenAud.count == 1 else {
            throw Abort(.unauthorized, reason: "invalid token: aud field is few or too much: \(tokenString)")
        }
        guard let tenant = try await retrieveTenantRowFromHeader(req: req) else {
            throw Abort(.unauthorized, reason: "tenant not found")
        }
        if tenant.name == "admin" && role != .admin {
            throw Abort(.unauthorized, reason: "tenant not found")
        }

        guard tenant.name == tokenAud[0] else {
            throw Abort(.unauthorized, reason: "invalid token: tenant name is not match with \(req.url.host ?? ""): \(tokenString)")
        }
        
        return Viewer(role: role, playerID: tokenSub, tenantName: tenant.name, tenantID: tenant.id)
    }
    
    func retrieveTenantRowFromHeader(req: Request) async throws -> TenantRow? {
        // JWTに入っているテナント名とHostヘッダのテナント名が一致しているか確認
        let baseHost = getEnv(key: "ISUCON_BASE_HOSTNAME", defaultValue: ".t.isucon.dev")
        let host = req.url.host ?? ""
        let tenantName = host.hasSuffix(baseHost)
            ? String(host[host.startIndex ..< host.index(host.endIndex, offsetBy: -baseHost.count)])
            : host
        
        // SaaS管理者用ドメイン
        if tenantName == "admin" {
            return TenantRow(id: 0, name: "admin", display_name: "admin", created_at: 0, updated_at: 0)
        }

        // テナントの存在確認
        return try await adminDB.sql().execute(
            "SELECT * FROM tenant WHERE name = \(bind: tenantName)"
        ).first(decoding: TenantRow.self)
    }
    
    struct TenantRow: Decodable {
        var id: Int64
        var name: String
        var display_name: String
        var created_at: Int64
        var updated_at: Int64
    }
    
    struct PlayerRow: Decodable {
        var tenant_id: Int64
        var id: String
        var display_name: String
        var is_disqualified: Bool
        var created_at: Int64
        var updated_at: Int64
    }
    
    // 参加者を取得する
    func retrievePlayer(tenantDB: some SQLDatabase, id: String) async throws -> PlayerRow? {
        try await tenantDB.execute(
            "SELECT * FROM player WHERE id = \(bind: id)"
        ).first(decoding: PlayerRow.self)
    }
    
    // 参加者を認可する
    // 参加者向けAPIで呼ばれる
    func authorizePlayer(tenantDB: some SQLDatabase, id: String) async throws {
        let player = try await retrievePlayer(tenantDB: tenantDB, id: id)
        guard let player else {
            throw Abort(.unauthorized, reason: "player not found")
        }
        if player.is_disqualified {
            throw Abort(.forbidden, reason: "player is disqualified")
        }
    }
    
    struct CompetitionRow: Decodable  {
        var tenant_id: Int64
        var id: String
        var title: String
        var finished_at: Int64?
        var created_at: Int64
        var updated_at: Int64
    }
    
    // 大会を取得する
    func retrieveCompetition(tenantDB: some SQLDatabase, id: String) async throws -> CompetitionRow? {
        try await tenantDB.execute(
            "SELECT * FROM competition WHERE id = \(bind: id)"
        ).first(decoding: CompetitionRow.self)
    }
    
    struct TenantsAddResult: Encodable {
        var tenant: TenantWithBilling
    }
    
    // SasS管理者用API
    // テナントを追加する
    // POST /api/admin/tenants/add
    func tenantsAdd(req: Request) async throws -> TenantsAddResult {
        fatalError("TODO")
    }
    
    struct TenantWithBilling: Encodable  {
        var id: String
        var name: String
        var display_name: String
        var billing: Int64
    }
}

@main struct Main {
    static func main() async throws {
        LoggingSystem.bootstrap { label in
            var handler = ConsoleLogger(label: label, console: Terminal())
            handler.logLevel = .debug
            return handler
        }
        
        let (pool, shutdown) = connectAdminDB()
        defer { try! shutdown() }
        
        let threadPool = NIOThreadPool(numberOfThreads: 16)
        threadPool.start()
        defer { try! threadPool.syncShutdownGracefully() }
        
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
        defer { try! eventLoopGroup.syncShutdownGracefully() }
        
        let server = Server(
            adminDB: pool.database(logger: .init(label: "adminDB")),
            threadPool: threadPool,
            eventLoopGroup: eventLoopGroup
        )
        
        try await server.run(port: Int(getEnv(key: "SERVER_APP_PORT", defaultValue: "3000"))!)
    }
}

extension NIOThreadPool {
    func task<T>(_ task: @escaping @Sendable () throws -> T) async throws -> T {
        try await withCheckedThrowingContinuation { c in
            submit { state in
                if state == .cancelled {
                    c.resume(throwing: CancellationError())
                } else {
                    c.resume(with: .init(catching: {
                        try task()
                    }))
                }
            }
        }
    }
}

extension SQLDatabase {
    func execute(_ query: SQLQueryString) -> SQLExecuteBuilder {
        SQLExecuteBuilder(query: query, database: self)
    }
}

final class SQLExecuteBuilder: SQLQueryFetcher, Sendable {
    let query: any SQLExpression
    let database: any SQLDatabase

    init(query: SQLQueryString, database: any SQLDatabase) {
        self.query = query
        self.database = database
    }
}
