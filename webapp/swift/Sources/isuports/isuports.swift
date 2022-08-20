@preconcurrency import MySQLKit
@preconcurrency import SQLiteKit
@preconcurrency import SQLKit
import JWTKit
@preconcurrency import TSCBasic
import Vapor

let tenantDBSchemaFilePath = "../sql/tenant/10_schema.sql"
let initializeScript = "../sql/init.sh"
let cookieName = "isuports_session"
let tenantNameRegexp = #/^[a-z][a-z0-9-]{0,61}[a-z0-9]$/#

enum Role: String, Codable {
    case admin
    case organizer
    case player
}

// 環境変数を取得する、なければデフォルト値を返す
func getEnv(key: String, defaultValue: String) -> String {
    ProcessInfo.processInfo.environment[key, default: defaultValue]
}

// 管理用DBに接続する
func connectAdminDB() -> (EventLoopConnectionPool<MySQLConnectionSource>, some Closable) {
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
    
    struct Context: Closable {
        var pool: EventLoopConnectionPool<MySQLConnectionSource>
        var eventLoopGroup: MultiThreadedEventLoopGroup
        func close() throws {
            try pool.close().wait()
            try eventLoopGroup.syncShutdownGracefully()
        }
    }
    let context = Context(pool: pool, eventLoopGroup: eventLoopGroup)
    return (context.pool, context)
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

@main struct Main {
    static func main() async throws {
        LoggingSystem.bootstrap { label in
            var handler = ConsoleLogger(label: label, console: Terminal())
            handler.logLevel = .debug
            return handler
        }
        
        let (pool, shutdown) = connectAdminDB()
        defer { try! shutdown.close() }
        
        let threadPool = NIOThreadPool(numberOfThreads: 16)
        threadPool.start()
        defer { try! threadPool.syncShutdownGracefully() }
        
        let app = try Application(.detect())
        defer { app.shutdown() }
        
        app.http.server.configuration.port = Int(getEnv(key: "SERVER_APP_PORT", defaultValue: "3000"))!
        app.middleware.use(CacheControlPrivateMiddleware())
        
        func route(_ route: @escaping @Sendable (Handler.Type) -> ((Handler) -> () async throws -> Response)) -> (Request) async throws -> Response {
            { request in
                let handler = Handler(
                    request: request,
                    adminDB: pool.database(logger: request.logger),
                    threadPool: threadPool,
                    eventLoopGroup: request.eventLoop
                )
                return try await route(Handler.self)(handler)()
            }
        }
        
        // SaaS管理者向けAPI
        app.post("/api/admin/tenants/add", use: route { $0.tenantsAdd })
        app.get("/api/admin/tenants/billing", use: route { $0.tenantsBilling })

        // テナント管理者向けAPI - 参加者追加、一覧、失格
        app.get("/api/organizer/players", use: route { $0.playersList })
        app.post("/api/organizer/players/add", use: route { $0.playersAdd })
        app.post("/api/organizer/player/:player_id/disqualified", use: route { $0.playerDisqualified })

        // テナント管理者向けAPI - 大会管理
        app.post("/api/organizer/competitions/add", use: route { $0.competitionsAdd })
        app.post("/api/organizer/competition/:competition_id/finish", use: route { $0.competitionFinish })
        app.post("/api/organizer/competition/:competition_id/score", use: route { $0.competitionScore })
        app.get("/api/organizer/billing", use: route { $0.billing })
        app.get("/api/organizer/competitions", use: route { $0.organizerCompetitions })

        // 参加者向けAPI
        app.get("/api/player/player/:player_id", use: route { $0.player })
        app.get("/api/player/competition/:competition_id/ranking", use: route { $0.competitionRanking })
        app.get("/api/player/competitions", use: route { $0.playerCompetitions })

        // 全ロール及び未認証でも使えるhandler
        app.get("/api/me", use: route { $0.me })

        // ベンチマーカー向けAPI
        app.post("/initialize", use: route { $0.initialize })
        
        try app.start()
        try await app.running?.onStop.get()
    }
}

struct Empty: Encodable {}

struct SuccessResult<T: Encodable>: Encodable {
    let status: Bool = true
    var data: T?
    
    init(data: T) {
        self.data = data
    }
    init() where T == Empty {
        self.data = nil
    }
}

struct FailureResult: Encodable  {
    let status: Bool = false
    var message: String
}

struct Handler {
    var request: Request
    var adminDB: any MySQLDatabase
    var threadPool: NIOThreadPool
    var eventLoopGroup: any EventLoopGroup
    init(request: Request, adminDB: any MySQLDatabase, threadPool: NIOThreadPool, eventLoopGroup: any EventLoopGroup) {
        self.request = request
        self.adminDB = adminDB
        self.threadPool = threadPool
        self.eventLoopGroup = eventLoopGroup
    }
    
    // テナントDBのパスを返す
    nonisolated func tenantDBPath(id: Int) -> RelativePath {
        let tenantDBDir = getEnv(key: "ISUCON_TENANT_DB_DIR", defaultValue: "../tenant_db")
        return RelativePath(tenantDBDir)
            .appending(component: "\(id).db")
    }
    
    // テナントDBに接続する
    func connectToTenantDB(id: Int) async throws -> SQLiteConnection {
        try await SQLiteConnection.open(
            storage: .file(path: tenantDBPath(id: id).pathString),
            threadPool: threadPool,
            on: eventLoopGroup.next()
        ).get()
    }
    
    // テナントDBを新規に作成する
    func createTenantDB(id: Int) async throws {
        let path = tenantDBPath(id: id)
        try await threadPool.task {
            let result = try Process.popen(args: "sh", "-c", "sqlite3 \(path) < \(tenantDBSchemaFilePath)")
            guard result.exitStatus == .terminated(code: 0) else {
                throw StringError("failed to exec sqlite3 \(path) < \(tenantDBSchemaFilePath), out=\(try result.utf8Output()), err=\(try result.utf8stderrOutput())")
            }
        }
    }
    
    // システム全体で一意なIDを生成する
    func dispenseID() async throws -> String {
        var lastError: Error?
        for _ in 0..<100 {
            var lastInsertID: UInt64?
            do {
                try await adminDB
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
    func parseViewer() async throws -> Viewer {
        guard let cookie = request.cookies[cookieName] else {
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
        guard let tenant = try await retrieveTenantRowFromHeader() else {
            throw Abort(.unauthorized, reason: "tenant not found")
        }
        if tenant.name == "admin" && role != .admin {
            throw Abort(.unauthorized, reason: "tenant not found")
        }
        
        guard tenant.name == tokenAud[0] else {
            throw Abort(.unauthorized, reason: "invalid token: tenant name is not match with \(request.url.host ?? ""): \(tokenString)")
        }
        
        return Viewer(role: role, playerID: tokenSub, tenantName: tenant.name, tenantID: tenant.id)
    }
    
    func retrieveTenantRowFromHeader() async throws -> TenantRow? {
        // JWTに入っているテナント名とHostヘッダのテナント名が一致しているか確認
        let baseHost = getEnv(key: "ISUCON_BASE_HOSTNAME", defaultValue: ".t.isucon.dev")
        let host = request.url.host ?? ""
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
    
    struct PlayerScoreRow: Decodable {
        var tenant_id: Int64
        var id: String
        var player_id: String
        var competition_id: String
        var score: Int64
        var row_num: Int64
        var created_at: Int64
        var updated_at: Int64
    }
    
    // 排他ロックのためのファイル名を生成する
    func lockFilePath(id: Int64) -> RelativePath {
        let tenantDBDir = getEnv(key: "ISUCON_TENANT_DB_DIR", defaultValue: "../tenant_db")
        return RelativePath(tenantDBDir)
            .appending(component: "\(id).lock")
    }
    
    // 排他ロックする
    func flockByTenantID<T>(tenantID: Int64, _ body: () throws -> T) throws -> T {
        let p = lockFilePath(id: tenantID)
        guard let pwd = localFileSystem.currentWorkingDirectory else {
            throw StringError("error localFileSystem.currentWorkingDirectory")
        }
        let fl = FileLock(at: AbsolutePath(pwd, p))
        return try fl.withLock(type: .exclusive, body)
    }
    
    struct TenantsAddResult: Encodable {
        var tenant: TenantWithBilling
    }
    
    // SasS管理者用API
    // テナントを追加する
    // POST /api/admin/tenants/add
    func tenantsAdd() async throws -> Response {
        return try .json(content: SuccessResult())
    }
    
    // テナント名が規則に沿っているかチェックする
    func validateTenantName(name: String) throws {
        if !name.matches(of: tenantNameRegexp).isEmpty {
            return
        }
        throw StringError("invalid tenant name: \(name)")
    }
    
    struct BillingReport: Encodable {
        var competition_id: String
        var competition_title: String
        var player_count: Int64        // スコアを登録した参加者数
        var visitor_count: Int64       // ランキングを閲覧だけした(スコアを登録していない)参加者数
        var billing_player_yen: Int64  // 請求金額 スコアを登録した参加者分
        var billing_visitor_yen: Int64 // 請求金額 ランキングを閲覧だけした(スコアを登録していない)参加者分
        var billing_yen: Int64         // 合計請求金額
    }
    
    struct VisitHistoryRow: Decodable {
        var player_id: String
        var tenant_id: Int64
        var competition_id: String
        var created_at: Int64
        var updated_at: Int64
    }
    
    struct VisitHistorySummaryRow: Decodable {
        var player_id: String
        var min_created_at: Int64
    }
    
    // 大会ごとの課金レポートを計算する
    func billingReportByCompetition(tenantDB: some SQLDatabase, tenantID: Int64, competitonID: String) async throws -> BillingReport {
        fatalError("TODO")
    }
    
    struct TenantWithBilling: Encodable {
        var id: String
        var name: String
        var display_name: String
        var billing: Int64
    }
    
    struct TenantsBillingResult: Encodable {
        var tenants: [TenantWithBilling]
    }
    
    // SaaS管理者用API
    // テナントごとの課金レポートを最大10件、テナントのid降順で取得する
    // GET /api/admin/tenants/billing
    // URL引数beforeを指定した場合、指定した値よりもidが小さいテナントの課金レポートを取得する
    func tenantsBilling() async throws -> Response {
        return try .json(content: SuccessResult())
    }
    
    struct PlayerDetail: Encodable {
        var id: String
        var display_name: String
        var is_disqualified: Bool
    }
    
    struct PlayersListResult: Encodable {
        var players: [PlayerDetail]
    }
    
    // テナント管理者向けAPI
    // GET /api/organizer/players
    // 参加者一覧を返す
    func playersList() async throws -> Response {
        return try .json(content: SuccessResult())
    }
    
    struct PlayersAddResult: Encodable {
        var players: PlayerDetail
    }
    
    // テナント管理者向けAPI
    // GET /api/organizer/players/add
    // テナントに参加者を追加する
    func playersAdd() async throws -> Response {
        return try .json(content: SuccessResult())
    }
    
    struct PlayerDisqualifiedResult: Encodable {
        var player: PlayerDetail
    }
    
    // テナント管理者向けAPI
    // POST /api/organizer/player/:player_id/disqualified
    // 参加者を失格にする
    func playerDisqualified() async throws -> Response {
        return try .json(content: SuccessResult())
    }
    
    struct CompetitionDetail: Encodable {
        var id: String
        var title: String
        var is_finished: Bool
    }
    
    struct CompetitionsAddResult: Encodable {
        var competition: CompetitionDetail
    }
    
    // テナント管理者向けAPI
    // POST /api/organizer/competitions/add
    // 大会を追加する
    func competitionsAdd() async throws -> Response {
        return try .json(content: SuccessResult())
    }
    
    // テナント管理者向けAPI
    // POST /api/organizer/competition/:competition_id/finish
    // 大会を終了する
    func competitionFinish() async throws -> Response {
        return try .json(content: SuccessResult())
    }
    
    struct ScoreResult: Encodable {
        var rows: Int64
    }
    
    // テナント管理者向けAPI
    // POST /api/organizer/competition/:competition_id/score
    // 大会のスコアをCSVでアップロードする
    func competitionScore() async throws -> Response {
        return try .json(content: SuccessResult())
    }
    
    struct BillingResult: Encodable {
        var reports: [BillingReport]
    }
    
    // テナント管理者向けAPI
    // GET /api/organizer/billing
    // テナント内の課金レポートを取得する
    func billing() async throws -> Response {
        return try .json(content: SuccessResult())
    }
    
    struct PlayerScoreDetail: Encodable {
        var competition_title: String
        var score: Int64
    }
    
    struct PlayerResult: Encodable {
        var player: PlayerDetail
        var scores: [PlayerScoreDetail]
    }
    
    // 参加者向けAPI
    // GET /api/player/player/:player_id
    // 参加者の詳細情報を取得する
    func player() async throws -> Response {
        return try .json(content: SuccessResult())
    }
    
    struct CompetitionRank: Encodable {
        var rank: Int64
        var score: Int64
        var player_id: String
        var player_display_name: String
        var rowNum: Int64 // APIレスポンスのJSONには含まれない
        
        enum CodingKeys: CodingKey {
            case rank
            case score
            case player_id
            case player_display_name
        }
    }
    
    struct CompetitionRankingResult: Encodable {
        var competition: CompetitionDetail
        var ranks: [CompetitionRank]
    }
    
    // 参加者向けAPI
    // GET /api/player/competition/:competition_id/ranking
    // 大会ごとのランキングを取得する
    func competitionRanking() async throws -> Response {
        return try .json(content: SuccessResult())
    }

    struct CompetitionsResult: Encodable {
        var competitions: [CompetitionDetail]
    }
    
    // 参加者向けAPI
    // GET /api/player/competitions
    // 大会の一覧を取得する
    func playerCompetitions() async throws -> Response {
        return try .json(content: SuccessResult())
    }
    
    // テナント管理者向けAPI
    // GET /api/organizer/competitions
    // 大会の一覧を取得する
    func organizerCompetitions() async throws -> Response {
        return try .json(content: SuccessResult())
    }
    
    func competitions(viewer: Viewer, tenantDB: some SQLDatabase) async throws -> Response {
        fatalError("TODO")
    }
    
    struct TenantDetail: Encodable {
        var name: String
        var display_name: String
    }

    struct MeResult: Encodable {
        var tenant: TenantDetail?
        var me: PlayerDetail?
        var role: Role
        var logged_in: Bool
    }
    
    // 共通API
    // GET /api/me
    // JWTで認証した結果、テナントやユーザ情報を返す
    func me() async throws -> Response {
        return try .json(content: SuccessResult())
    }
    
    struct InitializeResult: Encodable {
        var lang: String
    }
    
    // ベンチマーカー向けAPI
    // POST /initialize
    // ベンチマーカーが起動したときに最初に呼ぶ
    // データベースの初期化などが実行されるため、スキーマを変更した場合などは適宜改変すること
    func initialize() async throws -> Response {
        return try .json(content: SuccessResult())
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

extension Response {
    static func json(content: some Encodable) throws -> Response {
        let response = Response()
        try response.content.encode(content, as: .json)
        return response
    }
}
