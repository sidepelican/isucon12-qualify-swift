import CSV
import MySQLKit
import SQLiteKit
import JWTKit
import TSCBasic
import Vapor
import gperftoolsSwift

let tenantDBSchemaFilePath = "../sql/tenant/10_schema.sql"
let initializeScript = "../sql/init.sh"
let cookieName = "isuports_session"
let tenantNameRegexp = #/^[a-z][a-z0-9-]{0,61}[a-z0-9]$/#

enum Role: String, Codable {
    case admin
    case organizer
    case player
}

struct InternalError: CustomStringConvertible, Error {
    var description: String
    init(_ description: String) {
        self.description = description
    }
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

func errorResponseHandler(request: Request, error: any Error) -> Response {
    request.logger.report(error: error)

    let status: HTTPResponseStatus
    switch error {
    case let abort as AbortError:
        status = abort.status
    default:
        status = .internalServerError
    }
        
    return try! .json(status: status, content: FailureResult(message: ""))
}

final class MemCache<Key: Hashable, Value> {
    private var storage: [Key: Value] = [:]
    private let lock = Lock()
    
    subscript(_ key: Key) -> Value? {
        get {
            lock.withLock {
                storage[key]
            }
        }
    }
    func set(key: Key, value: Value) {
        lock.withLockVoid {
            storage[key] = value
        }
    }
    func remove(key: Key) {
        lock.withLockVoid {
            storage.removeValue(forKey: key)
        }
    }
}

struct Pair<F: Hashable & Sendable, S: Hashable & Sendable>: Hashable, Sendable {
    var first: F
    var second: S
    init(_ first: F, _ second: S) {
        self.first = first
        self.second = second
    }
}

@main struct Main {
    static func main() throws {
        LoggingSystem.bootstrap { label in
            var handler = ConsoleLogger(label: label, console: Terminal())
            handler.logLevel = .error
            return handler
        }
        
        let (pool, shutdown) = connectAdminDB()
        defer { try! shutdown.close() }
        
        let threadPool = NIOThreadPool(numberOfThreads: 14)
        threadPool.start()
        defer { try! threadPool.syncShutdownGracefully() }
        
        let threadPool2 = NIOThreadPool(numberOfThreads: 2)
        threadPool2.start()
        defer { try! threadPool2.syncShutdownGracefully() }
        
        let app = try Application(.detect())
        defer { app.shutdown() }
        
        app.http.server.configuration.port = Int(getEnv(key: "SERVER_APP_PORT", defaultValue: "3000"))!
        app.middleware = {
            var middlewares = Middlewares()
            middlewares.use(RouteLoggingMiddleware(logLevel: .info))
            middlewares.use(ErrorMiddleware(errorResponseHandler))
            middlewares.use(CacheControlPrivateMiddleware())
            return middlewares
        }()
        app.routes.defaultMaxBodySize = "1mb"
        
        let keyFilename = getEnv(key: "ISUCON_JWT_KEY_FILE", defaultValue: "../public.pem")
        let keysrc = try Data(contentsOf: URL(fileURLWithPath: keyFilename))
        let signer = JWTSigner.rs256(key: try .public(pem: keysrc))
    
        func route(_ route: @escaping (Handler.Type) -> (Handler) -> () -> EventLoopFuture<Response>) -> (Request) -> EventLoopFuture<Response> {
            { request in
                var logger = request.logger
                logger.logLevel = .warning
                let handler = Handler(
                    request: request,
                    adminDB: pool.database(logger: logger),
                    threadPool: threadPool,
                    threadPool2: threadPool2,
                    eventLoop: request.eventLoop,
                    signer: signer
                )
                return route(Handler.self)(handler)()
            }
        }
        
        // SaaS管理者向けAPI
        app.post("api", "admin", "tenants", "add", use: route { $0.tenantsAdd })
        app.get("api", "admin", "tenants", "billing", use: route { $0.tenantsBilling })

        // テナント管理者向けAPI - 参加者追加、一覧、失格
        app.get("api", "organizer", "players", use: route { $0.playersList })
        app.post("api", "organizer", "players", "add", use: route { $0.playersAdd })
        app.post("api", "organizer", "player", ":player_id", "disqualified", use: route { $0.playerDisqualified })

        // テナント管理者向けAPI - 大会管理
        app.post("api", "organizer", "competitions", "add", use: route { $0.competitionsAdd })
        app.post("api", "organizer", "competition", ":competition_id", "finish", use: route { $0.competitionFinish })
        app.post("api", "organizer", "competition", ":competition_id", "score", use: route { $0.competitionScore })
        app.get("api", "organizer", "billing", use: route { $0.billing })
        app.get("api", "organizer", "competitions", use: route { $0.organizerCompetitions })

        // 参加者向けAPI
        app.get("api", "player", "player", ":player_id", use: route { $0.player })
        app.get("api", "player", "competition", ":competition_id", "ranking", use: route { $0.competitionRanking })
        app.get("api", "player", "competitions", use: route { $0.playerCompetitions })

        // 全ロール及び未認証でも使えるhandler
        app.get("api", "me", use: route { $0.me })

        // ベンチマーカー向けAPI
        app.post("initialize", use: route { $0.initialize })
        
        let profilePath = NSTemporaryDirectory() + "a.profile"
        app.get("profile") { (req) -> EventLoopFuture<Response> in
            let waitSecond = min((try? req.query.get(TimeInterval.self, at: "s")) ?? 30, 120)
            Profiler.start(fname: profilePath)

            let promise = req.eventLoop.makePromise(of: Response.self)
            let req = UncheckedBox(value: req)
            DispatchQueue.global().asyncAfter(deadline: .now() + waitSecond) {
                Profiler.stop()
                let res = req.value.fileio.streamFile(at: profilePath)
                promise.completeWith(.success(res))
            }
            return promise.futureResult
        }

        app.get("binary") { (req) -> Response in
            return req.fileio.streamFile(at: ProcessInfo.processInfo.arguments[0])
        }
        
        try app.run()
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
    var threadPool2: NIOThreadPool
    var eventLoop: any EventLoop
    var signer: JWTSigner
    
    // テナントDBのパスを返す
    func tenantDBPath(id: Int64) -> AbsolutePath {
        let tenantDBDir = getEnv(key: "ISUCON_TENANT_DB_DIR", defaultValue: "../tenant_db")
        let pwd = AbsolutePath(FileManager.default.currentDirectoryPath)
        return .init(tenantDBDir, relativeTo: pwd)
            .appending(component: "\(id).db")
    }
    
    enum TenantDBPriority {
        case high
        case low
    }
    
    // テナントDBに接続する
    func connectToTenantDB<T>(id: Int64, priority: TenantDBPriority = .low, _ closure: @escaping (SQLiteConnection) throws -> EventLoopFuture<T>) -> EventLoopFuture<T> {
        var logger = request.logger
        logger.logLevel = .warning
        
        let threadPool: NIOThreadPool
        switch priority {
        case .low: threadPool = self.threadPool
        case .high: threadPool = self.threadPool2
        }
        
        return SQLiteConnection.open(
            storage: .file(path: tenantDBPath(id: id).pathString),
            threadPool: threadPool,
            logger: logger,
            on: eventLoop
        )
        .tryFlatMap { conn in
            try closure(conn)
                .flatMapAlways { (result) in
                    return conn.close()
                        .flatMapThrowing { () in
                            try result.get()
                        }
                }
        }
    }
    
    // テナントDBを新規に作成する
    func createTenantDB(id: Int64) -> EventLoopFuture<Void> {
        let path = tenantDBPath(id: id)
        return threadPool.runIfActive(eventLoop: eventLoop) {
            let result = try Process.popen(args: "sh", "-c", "sqlite3 \(path) < \(tenantDBSchemaFilePath)")
            guard result.exitStatus == .terminated(code: 0) else {
                throw InternalError("failed to exec sqlite3 \(path) < \(tenantDBSchemaFilePath), out=\(try result.utf8Output()), err=\(try result.utf8stderrOutput())")
            }
        }
    }
    
    // システム全体で一意なIDを生成する
    func dispenseID() -> String {
        return UUID().uuidString
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
        var exp: ExpirationClaim
        
        func verify(using signer: JWTSigner) throws {
            try exp.verifyNotExpired()
            // 他言語の挙動に寄せるために検査はparseViewer内で行い、CodableやJWTKitの仕組みにあまり乗っからない
        }
    }

    // リクエストヘッダをパースしてViewerを返す
    func parseViewer() -> EventLoopFuture<Viewer> {
        eventLoop.flatSubmit {
            guard let cookie = request.cookies[cookieName] else {
                throw Abort(.unauthorized, reason: "cookie \(cookieName) is not found")
            }
            let tokenString = cookie.string
            
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
            
            return retrieveTenantRowFromHeader()
                .unwrap(orError: Abort(.unauthorized, reason: "tenant not found"))
                .flatMapThrowing { tenant in
                    if tenant.name == "admin" && role != .admin {
                        throw Abort(.unauthorized, reason: "tenant not found")
                    }
                    
                    guard tenant.name == tokenAud[0] else {
                        throw Abort(.unauthorized, reason: "invalid token: tenant name is not match with \(request.url.host ?? ""): \(tokenString)")
                    }
                    
                    return Viewer(role: role, playerID: tokenSub, tenantName: tenant.name, tenantID: tenant.id)
                }
        }
    }
    
    func retrieveTenantRowFromHeader() -> EventLoopFuture<TenantRow?> {
        // JWTに入っているテナント名とHostヘッダのテナント名が一致しているか確認
        let baseHost = getEnv(key: "ISUCON_BASE_HOSTNAME", defaultValue: ".t.isucon.dev")
        let host = request.headers.first(name: .host) ?? ""
        let tenantName = host.hasSuffix(baseHost)
        ? String(host[host.startIndex ..< host.index(host.endIndex, offsetBy: -baseHost.count)])
        : host
        
        // SaaS管理者用ドメイン
        if tenantName == "admin" {
            return eventLoop.future(TenantRow(id: 0, name: "admin", display_name: "admin", created_at: 0, updated_at: 0))
        }
        
        // テナントの存在確認
        return adminDB.sql().execute(
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
    func retrievePlayer(tenantDB: some SQLDatabase, id: String) -> EventLoopFuture<PlayerRow?> {
        return tenantDB.execute(
            "SELECT * FROM player WHERE id = \(bind: id)"
        ).first(decoding: PlayerRow.self)
    }
    
    // 参加者を認可する
    // 参加者向けAPIで呼ばれる
    func authorizePlayer(tenantDB: some SQLDatabase, id: String) -> EventLoopFuture<Void> {
        retrievePlayer(tenantDB: tenantDB, id: id).flatMapThrowing { player in
            guard let player else {
                throw Abort(.unauthorized, reason: "player not found")
            }
            if player.is_disqualified {
                throw Abort(.forbidden, reason: "player is disqualified")
            }
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
    func retrieveCompetition(tenantDB: some SQLDatabase, id: String) -> EventLoopFuture<CompetitionRow?> {
        tenantDB.execute(
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
    func lockFilePath(id: Int64) -> AbsolutePath {
        let tenantDBDir = getEnv(key: "ISUCON_TENANT_DB_DIR", defaultValue: "../tenant_db")
        let pwd = AbsolutePath(FileManager.default.currentDirectoryPath)
        return .init(tenantDBDir, relativeTo: pwd)
            .appending(component: "\(id).lock")
    }
    
    struct TenantsAddResult: Encodable {
        var tenant: TenantWithBilling
    }
    
    // SasS管理者用API
    // テナントを追加する
    // POST /api/admin/tenants/add
    func tenantsAdd() -> EventLoopFuture<Response> {
        return parseViewer().tryFlatMap { (v: Viewer) in
            guard v.tenantName == "admin" else {
                // admin: SaaS管理者用の特別なテナント名
                throw Abort(.notFound, reason: "\(v.tenantName) has not this API")
            }
            guard v.role == .admin else {
                throw Abort(.forbidden, reason: "admin role required")
            }
            
            struct Form: Decodable {
                var display_name: String
                var name: String
            }
            let form = try request.content.decode(Form.self)
            do {
                try validateTenantName(name: form.name)
            } catch {
                throw Abort(.badRequest, reason: "\(error)")
            }
            
            var lastInsertID: UInt64?
            let now = Int64(Date().timeIntervalSince1970)
            return adminDB
                .query(
                    "INSERT INTO tenant (name, display_name, created_at, updated_at) VALUES (?, ?, ?, ?)",
                    [.init(string: form.name), .init(string: form.display_name), .init(int: Int(now)),  .init(int: Int(now))],
                    onRow: { _ in },
                    onMetadata: { metadata in
                        lastInsertID = metadata.lastInsertID
                    }
                )
                .flatMapErrorThrowing { error in
                    if let mysqlError = error as? MySQLError,
                       case .duplicateEntry = mysqlError {
                        throw Abort(.badRequest, reason: "duplicate tenant")
                    } else {
                        throw error
                    }
                }
                .tryFlatMap { () in
                    guard let id = lastInsertID.map({ Int64(clamping: $0) }) else {
                        throw InternalError("error get lastInsertId")
                    }
                    
                    // NOTE: 先にadminDBに書き込まれることでこのAPIの処理中に
                    //       /api/admin/tenants/billingにアクセスされるとエラーになりそう
                    //       ロックなどで対処したほうが良さそう
                    return createTenantDB(id: id).flatMapThrowing {
                        let res = TenantsAddResult(
                            tenant: .init(
                                id: String(id),
                                name: form.name,
                                display_name: form.display_name,
                                billing: 0
                            )
                        )
                        return try Response.json(content: SuccessResult(data: res))
                    }
                }
        }
    }
    
    // テナント名が規則に沿っているかチェックする
    func validateTenantName(name: String) throws {
        guard !name.matches(of: tenantNameRegexp).isEmpty else {
            throw InternalError("invalid tenant name: \(name)")
        }
    }
    
    struct BillingReport: Encodable, Sendable {
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
    
    static let billingReportByCompetitionCache = MemCache<Pair<Int64, String>, BillingReport>()
    // 大会ごとの課金レポートを計算する
    func billingReportByCompetition(tenantDB: some SQLDatabase, tenantID: Int64, competitonID: String) -> EventLoopFuture<BillingReport> {
        if let cache = Self.billingReportByCompetitionCache[.init(tenantID, competitonID)] {
            return eventLoop.future(cache)
        }
        
        return retrieveCompetition(tenantDB: tenantDB, id: competitonID)
            .unwrap(orError: InternalError("error retrieveCompetition"))
            .tryFlatMap { comp in
                if comp.finished_at == nil {
                    return eventLoop.future(BillingReport(
                        competition_id: comp.id,
                        competition_title: comp.title,
                        player_count: 0,
                        visitor_count: 0,
                        billing_player_yen: 0,
                        billing_visitor_yen: 0,
                        billing_yen: 0
                    ))
                }
                
                // ランキングにアクセスした参加者のIDを取得する
                let histories: EventLoopFuture<[VisitHistorySummaryRow]> = adminDB.sql().execute(
                    "SELECT player_id, MIN(created_at) AS min_created_at FROM visit_history WHERE tenant_id = \(bind: tenantID) AND competition_id = \(bind: comp.id) GROUP BY player_id"
                ).all(decoding: VisitHistorySummaryRow.self)
                
                // スコアを登録した参加者のIDを取得する
                let scoredPlayerIDs: EventLoopFuture<[String]> = tenantDB.execute(
                    "SELECT DISTINCT(player_id) FROM player_score WHERE tenant_id = \(bind: tenantID) AND competition_id = \(bind: comp.id)"
                ).all(collecting: { (playerID: String) in playerID })
                
                return histories.and(scoredPlayerIDs).map { (histories, scoredPlayerIDs) in
                    var billingMap: [String: String] = [:]
                    for history in histories {
                        // competition.finished_atよりもあとの場合は、終了後に訪問したとみなして大会開催内アクセス済みとみなさない
                        if let finished_at = comp.finished_at, finished_at < history.min_created_at {
                            continue
                        }
                        billingMap[history.player_id] = "visitor"
                    }
                    
                    for pid in scoredPlayerIDs {
                        // スコアが登録されている参加者
                        billingMap[pid] = "player"
                    }
                    
                    // 大会が終了している場合のみ請求金額が確定するので計算する
                    var playerCount: Int64 = 0
                    var visitorCount: Int64 = 0
                    if comp.finished_at != nil {
                        for (_, category) in billingMap {
                            if category == "player" {
                                playerCount += 1
                            } else if category == "visitor" {
                                visitorCount += 1
                            }
                        }
                    }
                    
                    let ret = BillingReport(
                        competition_id: comp.id,
                        competition_title: comp.title,
                        player_count: playerCount,
                        visitor_count: visitorCount,
                        billing_player_yen: 100 * playerCount, // スコアを登録した参加者は100円
                        billing_visitor_yen: 10 * visitorCount, // ランキングを閲覧だけした(スコアを登録していない)参加者は10円
                        billing_yen: 100 * playerCount + 10 * visitorCount
                    )
                    
                    Self.billingReportByCompetitionCache.set(key: .init(tenantID, competitonID), value: ret)
                    return ret
                }
            }
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
    func tenantsBilling() -> EventLoopFuture<Response> {
        return eventLoop.flatSubmit {
            let host = request.headers.first(name: .host) ?? ""
            guard !host.isEmpty,
                  host == getEnv(key: "ISUCON_ADMIN_HOSTNAME", defaultValue: "admin.t.isucon.dev") else {
                throw Abort(.notFound, reason: "invalid hostname \(host)")
            }
            
            return parseViewer()
        }
        .tryFlatMap { (v: Viewer) in
            guard v.role == .admin else {
                throw Abort(.forbidden, reason: "admin role required")
            }
            
            struct Query: Decodable {
                var before: Int64?
            }
            let query = try request.query.decode(Query.self)
            let beforeID = query.before ?? 0
            
            // テナントごとに
            //   大会ごとに
            //     scoreが登録されているplayer * 100
            //     scoreが登録されていないplayerでアクセスした人 * 10
            //   を合計したものを
            // テナントの課金とする
            return adminDB.sql().execute(
                "SELECT * FROM tenant ORDER BY id DESC"
            ).all(decoding: TenantRow.self)
                .map { tenants in
                    tenants.filter { tenant in
                        !(beforeID != 0 && beforeID <= tenant.id)
                    }
                    .prefix(10)
                }
                .sequencedFlatMapEach { tenant -> EventLoopFuture<TenantWithBilling> in
                    return connectToTenantDB(id: tenant.id) { tenantDB in
                        var billingYen: Int64 = 0
                        return tenantDB.sql().execute(
                            "SELECT * FROM competition WHERE tenant_id=\(bind: tenant.id)"
                        ).all(decoding: CompetitionRow.self).sequencedFlatMapEach { comp in
                            billingReportByCompetition(tenantDB: tenantDB.sql(), tenantID: tenant.id, competitonID: comp.id)
                                .map { report in
                                    billingYen += report.billing_yen
                                    return ()
                                }
                        }.map { () in
                            return billingYen
                        }
                    }.map { billingYen in
                        TenantWithBilling(
                            id: String(tenant.id),
                            name: tenant.name,
                            display_name: tenant.display_name,
                            billing: billingYen
                        )
                    }
                }.flatMapThrowing { tenantBillings in
                    let res = TenantsBillingResult(
                        tenants: tenantBillings
                    )
                    return try .json(content: SuccessResult(data: res))
                }
        }
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
    func playersList() -> EventLoopFuture<Response> {
        return parseViewer().tryFlatMap { (v: Viewer) in
            guard v.role == .organizer else {
                throw Abort(.forbidden, reason: "role organizer required")
            }
            
            return connectToTenantDB(id: v.tenantID) { tenantDB in
                return tenantDB.sql().execute(
                    "SELECT * FROM player WHERE tenant_id=\(bind: v.tenantID) ORDER BY created_at DESC"
                ).all(decoding: PlayerRow.self).flatMapThrowing { players in
                    let res = PlayersListResult(
                        players: players.map { p in
                            PlayerDetail(id: p.id, display_name: p.display_name, is_disqualified: p.is_disqualified)
                        }
                    )
                    return try .json(content: SuccessResult(data: res))
                }
            }
        }
    }
    
    struct PlayersAddResult: Encodable {
        var players: [PlayerDetail]
    }
    
    // テナント管理者向けAPI
    // GET /api/organizer/players/add
    // テナントに参加者を追加する
    func playersAdd() -> EventLoopFuture<Response> {
        return parseViewer().tryFlatMap { (v: Viewer) in
            guard v.role == .organizer else {
                throw Abort(.forbidden, reason: "role organizer required")
            }
            
            return connectToTenantDB(id: v.tenantID) { tenantDB in
                struct Form: Decodable {
                    var display_name: [String]
                }
                let form = try request.content.decode(Form.self)
                let displayNames = form.display_name
                
                return displayNames.sequencedFlatMapEach(on: eventLoop) { displayName -> EventLoopFuture<PlayerDetail> in
                    let id = dispenseID()
                    
                    let now = Int64(Date().timeIntervalSince1970)
                    return tenantDB.sql().execute(
                        "INSERT INTO player (id, tenant_id, display_name, is_disqualified, created_at, updated_at) VALUES (\(bind: id), \(bind: v.tenantID), \(bind: displayName), \(bind: false), \(bind: now), \(bind: now))"
                    ).run().flatMap { () in
                        retrievePlayer(tenantDB: tenantDB.sql(), id: id)
                            .unwrap(orError: InternalError("error retrievePlayer"))
                            .map { p in
                                PlayerDetail(
                                    id: p.id,
                                    display_name: p.display_name,
                                    is_disqualified: p.is_disqualified
                                )
                            }
                    }
                }.flatMapThrowing { playerDetails in
                    let res = PlayersAddResult(players: playerDetails)
                    return try .json(content: SuccessResult(data: res))
                }
            }
        }
    }
    
    struct PlayerDisqualifiedResult: Encodable {
        var player: PlayerDetail
    }
    
    // テナント管理者向けAPI
    // POST /api/organizer/player/:player_id/disqualified
    // 参加者を失格にする
    func playerDisqualified() -> EventLoopFuture<Response> {
        return parseViewer().tryFlatMap { (v: Viewer) in
            guard v.role == .organizer else {
                throw Abort(.forbidden, reason: "role organizer required")
            }
            
            return connectToTenantDB(id: v.tenantID) { tenantDB in
                let playerID = request.parameters.get("player_id") ?? ""
                
                let now = Int64(Date().timeIntervalSince1970)
                return tenantDB.sql().execute(
                    "UPDATE player SET is_disqualified = \(bind: true), updated_at = \(bind: now) WHERE id = \(bind: playerID)"
                ).run().flatMap { () in
                    retrievePlayer(tenantDB: tenantDB.sql(), id: playerID)
                    // 存在しないプレイヤー
                        .unwrap(orError: Abort(.notFound, reason: "player not found"))
                }
            }
            .flatMapThrowing { p in
                let res = PlayerDisqualifiedResult(
                    player: PlayerDetail(
                        id: p.id,
                        display_name: p.display_name,
                        is_disqualified: p.is_disqualified
                    )
                )
                return try .json(content: SuccessResult(data: res))
            }
        }
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
    func competitionsAdd() -> EventLoopFuture<Response> {
        return parseViewer().tryFlatMap { (v: Viewer) in
            guard v.role == .organizer else {
                throw Abort(.forbidden, reason: "role organizer required")
            }
            
            return connectToTenantDB(id: v.tenantID) { tenantDB in
                struct Form: Decodable {
                    var title: String
                }
                let form = try request.content.decode(Form.self)
                let title = form.title
                
                let now = Int64(Date().timeIntervalSince1970)
                let id = dispenseID()
                return tenantDB.sql().execute(
                    "INSERT INTO competition (id, tenant_id, title, finished_at, created_at, updated_at) VALUES (\(bind: id), \(bind: v.tenantID), \(bind: title), \(bind: Int64?.none), \(bind: now), \(bind: now))"
                ).run().flatMapThrowing { () in
                    let res = CompetitionsAddResult(
                        competition: CompetitionDetail(
                            id: id,
                            title: title,
                            is_finished: false
                        )
                    )
                    return try .json(content: SuccessResult(data: res))
                }
            }
        }
    }
    
    // テナント管理者向けAPI
    // POST /api/organizer/competition/:competition_id/finish
    // 大会を終了する
    func competitionFinish() -> EventLoopFuture<Response> {
        return parseViewer().tryFlatMap { (v: Viewer) in
            guard v.role == .organizer else {
                throw Abort(.forbidden, reason: "role organizer required")
            }
            
            return connectToTenantDB(id: v.tenantID) { tenantDB in
                guard let id = request.parameters.get("competition_id"), id != "" else {
                    throw Abort(.badRequest, reason: "competition_id required")
                }
                
                return retrieveCompetition(tenantDB: tenantDB.sql(), id: id)
                    .unwrap(orError: Abort(.notFound, reason: "competition not found"))
                    .flatMap { (_) in
                        let now = Int64(Date().timeIntervalSince1970)
                        return tenantDB.sql().execute(
                            "UPDATE competition SET finished_at = \(bind: now), updated_at = \(bind: now) WHERE id = \(bind: id)"
                        ).run().flatMapThrowing {
                            return try Response.json(content: SuccessResult())
                        }
                    }
            }
        }
    }
    
    struct ScoreResult: Encodable {
        var rows: Int
    }
    
    // テナント管理者向けAPI
    // POST /api/organizer/competition/:competition_id/score
    // 大会のスコアをCSVでアップロードする
    func competitionScore() -> EventLoopFuture<Response> {
        return parseViewer().tryFlatMap { (v: Viewer) in
            guard v.role == .organizer else {
                throw Abort(.forbidden, reason: "role organizer required")
            }
            
            guard let competitionID = request.parameters.get("competition_id"), competitionID != "" else {
                throw Abort(.badRequest, reason: "competition_id required")
            }
            
            return connectToTenantDB(id: v.tenantID, priority: .high) { tenantDB in
                return retrieveCompetition(tenantDB: tenantDB.sql(), id: competitionID)
                    // 存在しない大会
                    .unwrap(orError: Abort(.notFound, reason: "competition not found"))
                    .tryFlatMap { comp -> EventLoopFuture<Response> in
                        if comp.finished_at != nil {
                            return eventLoop.future(try .json(status: .badRequest, content: FailureResult(
                                message: "competition is finished"
                            )))
                        }
                        
                        struct Form: Decodable {
                            var scores: File
                        }
                        let form = try request.content.decode(Form.self)
                        
                        let reader = try CSVReader(
                            stream: InputStream(data: Data(buffer: form.scores.data)),
                            hasHeaderRow: true
                        )
                        guard reader.headerRow == ["player_id", "score"] else {
                            throw Abort(.badRequest, reason: "invalid CSV headers")
                        }
                        
                        let csvRows = try reader.compactMap { (row) -> (String, Int64) in
                            guard row.count == 2 else {
                                throw InternalError("row must have two columns: \(row)")
                            }
                            let playerID = row[0], score = Int64(row[1])
                            guard let score else {
                                throw Abort(.badRequest, reason: "error Int64: scoreStr=\(row[1])")
                            }
                            return (playerID, score)
                        }
                        let uniquePlayerIDs = Set(csvRows.map(\.0))
                        return tenantDB.sql().execute(
                            "SELECT COUNT(*) FROM player WHERE id IN (\(binds: uniquePlayerIDs.map { $0 }))"
                        ).first(collecting: { (c: Int64) in c }).tryFlatMap { (count: Int64?) in
                            guard count! == uniquePlayerIDs.count else {
                                throw Abort(.badRequest, reason: "player not found")
                            }

                            let playerScoreRows: [PlayerScoreRow] = zip(Int64(1)..., csvRows).map { rowNum, row in
                                let id = dispenseID()
                                let now = Int64(Date().timeIntervalSince1970)
                                return PlayerScoreRow(
                                    tenant_id: v.tenantID,
                                    id: id,
                                    player_id: row.0,
                                    competition_id: competitionID,
                                    score: row.1,
                                    row_num: rowNum,
                                    created_at: now,
                                    updated_at: now
                                )
                            }
                            
                            return tenantDB.transaction { tenantDB in
                                return tenantDB.sql().execute(
                                    "DELETE FROM player_score WHERE tenant_id = \(bind: v.tenantID) AND competition_id = \(bind: competitionID)"
                                ).run().flatMap { () in
                                    if !playerScoreRows.isEmpty {
                                        let inserts = playerScoreRows.map { ps -> SQLQueryString in
                                            "(\(bind:ps.id), \(bind:ps.tenant_id), \(bind:ps.player_id), \(bind:ps.competition_id), \(bind:ps.score), \(bind:ps.row_num), \(bind:ps.created_at), \(bind:ps.updated_at))"
                                        }
                                        return tenantDB.sql().execute("""
                        INSERT INTO player_score (id, tenant_id, player_id, competition_id, score, row_num, created_at, updated_at)
                        VALUES \(inserts.joined(separator: ","));
                    """
                                        ).run()
                                    } else {
                                        return eventLoop.future()
                                    }
                                }
                                
                            }.flatMapThrowing { () in
                                let res = ScoreResult(rows: playerScoreRows.count)
                                return try .json(content: SuccessResult(data: res))
                            }
                        }
                    }
            }
        }
    }
    
    struct BillingResult: Encodable {
        var reports: [BillingReport]
    }
    
    // テナント管理者向けAPI
    // GET /api/organizer/billing
    // テナント内の課金レポートを取得する
    func billing() -> EventLoopFuture<Response> {
        return parseViewer().tryFlatMap { (v: Viewer) in
            guard v.role == .organizer else {
                throw Abort(.forbidden, reason: "role organizer required")
            }
            
            return connectToTenantDB(id: v.tenantID) { tenantDB in
                return tenantDB.sql().execute(
                    "SELECT * FROM competition WHERE tenant_id=\(bind: v.tenantID) ORDER BY created_at DESC"
                ).all(decoding: CompetitionRow.self)
                    .sequencedFlatMapEach { comp in
                        billingReportByCompetition(tenantDB: tenantDB.sql(), tenantID: v.tenantID, competitonID: comp.id)
                    }
                    .flatMapThrowing { reports in
                        let res = BillingResult(reports: reports)
                        return try .json(content: SuccessResult(data: res))
                    }
            }
        }
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
    func player() -> EventLoopFuture<Response> {
        return parseViewer().tryFlatMap { (v: Viewer) in
            guard v.role == .player else {
                throw Abort(.forbidden, reason: "role player required")
            }
            
            return connectToTenantDB(id: v.tenantID) { tenantDB in
                return authorizePlayer(tenantDB: tenantDB.sql(), id: v.playerID).tryFlatMap { () in
                    guard let playerID = request.parameters.get("player_id"), playerID != "" else {
                        throw Abort(.badRequest, reason: "player_id is required")
                    }
                    return retrievePlayer(tenantDB: tenantDB.sql(), id: playerID)
                        .unwrap(orError: Abort(.notFound, reason: "player not found"))
                        .flatMap { (p: PlayerRow) in
                            return tenantDB.sql().execute(
                                "SELECT * FROM competition WHERE tenant_id = \(bind: v.tenantID) ORDER BY created_at ASC"
                            ).all(decoding: CompetitionRow.self).sequencedFlatMapEachCompact { (c: CompetitionRow) -> EventLoopFuture<PlayerScoreRow?> in
                                // 最後にCSVに登場したスコアを採用する = row_numが一番大きいもの
                                // 行がない = スコアが記録されてない
                                tenantDB.sql().execute(
                                    "SELECT * FROM player_score WHERE tenant_id = \(bind: v.tenantID) AND competition_id = \(bind: c.id) AND player_id = \(bind: p.id) ORDER BY row_num DESC LIMIT 1"
                                ).first(decoding: PlayerScoreRow.self)
                            }.sequencedFlatMapEach { (ps: PlayerScoreRow) -> EventLoopFuture<PlayerScoreDetail> in
                                retrieveCompetition(tenantDB: tenantDB.sql(), id: ps.competition_id)
                                    .unwrap(orError: InternalError("error retrieveCompetition"))
                                    .map { comp in
                                        PlayerScoreDetail(
                                            competition_title: comp.title,
                                            score: ps.score
                                        )
                                    }
                            }.flatMapThrowing { (scoreDetails: [PlayerScoreDetail]) in
                                let res = PlayerResult(
                                    player: .init(
                                        id: p.id,
                                        display_name: p.display_name,
                                        is_disqualified: p.is_disqualified
                                    ),
                                    scores: scoreDetails
                                )
                                return try .json(content: SuccessResult(data: res))
                            }
                        }
                }
            }
        }
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
    func competitionRanking() -> EventLoopFuture<Response> {
        return parseViewer().tryFlatMap { (v: Viewer) in
            guard v.role == .player else {
                throw Abort(.forbidden, reason: "role player required")
            }
            
            return connectToTenantDB(id: v.tenantID) { tenantDB in
                return authorizePlayer(tenantDB: tenantDB.sql(), id: v.playerID).tryFlatMap { ()
                    guard let competitionID = request.parameters.get("competition_id"), competitionID != "" else {
                        throw Abort(.badRequest, reason: "competition_id required")
                    }
                    
                    // 大会の存在確認
                    return retrieveCompetition(tenantDB: tenantDB.sql(), id: competitionID)
                        .unwrap(orError: Abort(.notFound, reason: "competition not found"))
                        .flatMap { competition in
                            let insertFuture: EventLoopFuture<Void>
                            // 大会終了後のvisit_historyは使わないので記録をスキップ
                            if competition.finished_at == nil {
                                let now = Int64(Date().timeIntervalSince1970)
                                insertFuture = adminDB.sql().execute(
                                    "INSERT INTO visit_history (player_id, tenant_id, competition_id, created_at, updated_at) VALUES (\(bind: v.playerID), \(bind: v.tenantID), \(bind: competitionID), \(bind: now), \(bind: now))"
                                ).run()
                            } else {
                                insertFuture = eventLoop.future()
                            }
                            
                            return insertFuture.flatMap { () in
                                let rankAfter = (try? request.query.get(Int64.self, at: "rank_after")) ?? 0
                                
                                struct PlayerScoreRow: Decodable {
                                    var tenant_id: Int64
                                    var id: String
                                    var player_id: String
                                    var competition_id: String
                                    var score: Int64
                                    var row_num: Int64
                                    var created_at: Int64
                                    var updated_at: Int64
                                    
                                    var display_name: String
                                }
                                
                                return tenantDB.sql().execute("""
                SELECT player_score.*, player.display_name FROM player_score
                  JOIN player ON player.id = player_score.player_id
                  WHERE player_score.tenant_id = \(bind: v.tenantID) AND competition_id = \(bind: competitionID) ORDER BY row_num DESC;
            """).all(decoding: PlayerScoreRow.self).flatMapThrowing { playerScores in
                                    var ranks: [CompetitionRank] = []
                                    ranks.reserveCapacity(playerScores.count)
                                    var scoredPlayerSet: Set<String> = []
                                    scoredPlayerSet.reserveCapacity(playerScores.count)
                                    for ps in playerScores {
                                        // player_scoreが同一player_id内ではrow_numの降順でソートされているので
                                        // 現れたのが2回目以降のplayer_idはより大きいrow_numでスコアが出ているとみなせる
                                        if scoredPlayerSet.contains(ps.player_id) {
                                            continue
                                        }
                                        scoredPlayerSet.insert(ps.player_id)
                                        
                                        ranks.append(CompetitionRank(
                                            rank: 0,
                                            score: ps.score,
                                            player_id: ps.player_id,
                                            player_display_name: ps.display_name,
                                            rowNum: ps.row_num
                                        ))
                                    }
                                    ranks.sort { lhs, rhs in
                                        if lhs.score == rhs.score {
                                            return lhs.rowNum < rhs.rowNum
                                        }
                                        return lhs.score > rhs.score
                                    }
                                    let pagedRanks = ranks
                                        .dropFirst(numericCast(rankAfter))
                                        .prefix(100)
                                        .enumerated()
                                        .map { i, rank in
                                            CompetitionRank(
                                                rank: rankAfter + Int64(i) + 1,
                                                score: rank.score,
                                                player_id: rank.player_id,
                                                player_display_name: rank.player_display_name,
                                                rowNum: 0
                                            )
                                        }
                                    
                                    let res = CompetitionRankingResult(
                                        competition: .init(
                                            id: competition.id,
                                            title: competition.title,
                                            is_finished: competition.finished_at != nil
                                        ),
                                        ranks: pagedRanks
                                    )
                                    return try .json(content: SuccessResult(data: res))
                                }
                            }
                        }
                }
            }
        }
    }

    struct CompetitionsResult: Encodable {
        var competitions: [CompetitionDetail]
    }
    
    // 参加者向けAPI
    // GET /api/player/competitions
    // 大会の一覧を取得する
    func playerCompetitions() -> EventLoopFuture<Response> {
        return parseViewer().tryFlatMap { (v: Viewer) in
            guard v.role == .player else {
                throw Abort(.forbidden, reason: "role player required")
            }
            
            return connectToTenantDB(id: v.tenantID) { tenantDB in
                return authorizePlayer(tenantDB: tenantDB.sql(), id: v.playerID).flatMap {
                    return competitions(viewer: v, tenantDB: tenantDB.sql())
                }
            }
        }
    }
    
    // テナント管理者向けAPI
    // GET /api/organizer/competitions
    // 大会の一覧を取得する
    func organizerCompetitions() -> EventLoopFuture<Response> {
        return parseViewer().tryFlatMap { (v: Viewer) in
            guard v.role == .organizer else {
                throw Abort(.forbidden, reason: "role organizer required")
            }
            
            return connectToTenantDB(id: v.tenantID) { tenantDB in
                return competitions(viewer: v, tenantDB: tenantDB.sql())
            }
        }
    }
    
    func competitions(viewer: Viewer, tenantDB: some SQLDatabase) -> EventLoopFuture<Response> {
        tenantDB.execute(
            "SELECT * FROM competition WHERE tenant_id=\(bind: viewer.tenantID) ORDER BY created_at DESC"
        ).all(decoding: CompetitionRow.self).flatMapThrowing { competitions in
            let ret = CompetitionsResult(
                competitions: competitions.map { comp in
                    CompetitionDetail(
                        id: comp.id,
                        title: comp.title,
                        is_finished: comp.finished_at != nil
                    )
                }
            )
            return try .json(content: SuccessResult(data: ret))
        }
    }
    
    struct TenantDetail: Encodable {
        var name: String
        var display_name: String
    }

    struct MeResult: Encodable {
        var tenant: TenantDetail?
        var me: PlayerDetail?
        var role: String
        var logged_in: Bool
    }
    
    // 共通API
    // GET /api/me
    // JWTで認証した結果、テナントやユーザ情報を返す
    func me() -> EventLoopFuture<Response> {
        return retrieveTenantRowFromHeader()
            .unwrap(orError: InternalError("error retrieveTenantRowFromHeader"))
            .flatMap { tenant in
                let tenantDetail = TenantDetail(
                    name: tenant.name,
                    display_name: tenant.display_name
                )
                return parseViewer()
                    .flatMap { (v: Viewer) -> EventLoopFuture<Response> in
                        if v.role == .admin || v.role == .organizer {
                            return eventLoop.submit {
                                try .json(content: SuccessResult(data: MeResult(
                                    tenant: tenantDetail,
                                    me: nil,
                                    role: v.role.rawValue,
                                    logged_in: true
                                )))
                            }
                        }
                        
                        return connectToTenantDB(id: v.tenantID) { tenantDB in
                            return retrievePlayer(tenantDB: tenantDB.sql(), id: v.playerID)
                                .flatMapThrowing { p in
                                    guard let p else {
                                        return try .json(content: SuccessResult(data: MeResult(
                                            tenant: tenantDetail,
                                            me: nil,
                                            role: "none",
                                            logged_in: false
                                        )))
                                    }
                                    return try .json(content: SuccessResult(data: MeResult(
                                        tenant: tenantDetail,
                                        me: PlayerDetail(
                                            id: p.id,
                                            display_name: p.display_name,
                                            is_disqualified: p.is_disqualified
                                        ),
                                        role: v.role.rawValue,
                                        logged_in: true
                                    )))
                                }
                        }
                    }
                    .flatMapErrorThrowing { (error) -> Response in
                        if let error = error as? any AbortError, error.status == .unauthorized {
                            return try Response.json(content: SuccessResult(data: MeResult(
                                tenant: tenantDetail,
                                me: nil,
                                role: "none",
                                logged_in: false
                            )))
                        } else {
                            throw error
                        }
                    }
            }
    }
    
    struct InitializeResult: Encodable {
        var lang: String
    }
    
    // ベンチマーカー向けAPI
    // POST /initialize
    // ベンチマーカーが起動したときに最初に呼ぶ
    // データベースの初期化などが実行されるため、スキーマを変更した場合などは適宜改変すること
    func initialize() -> EventLoopFuture<Response> {
        threadPool.runIfActive(eventLoop: eventLoop) {
            let result = try Process.popen(args: initializeScript)
            guard result.exitStatus == .terminated(code: 0) else {
                throw InternalError("errro exec command: \(initializeScript), out=\(try result.utf8Output()), err=\(try result.utf8stderrOutput())")
            }
        }.flatMapThrowing { () in
            let res = InitializeResult(lang: "swift")
            return try .json(content: SuccessResult(data: res))
        }
    }
}

extension NIOThreadPool {
    func task<T: Sendable>(_ task: @escaping @Sendable () throws -> T) async throws -> T {
        try await withCheckedThrowingContinuation { c in
            submit { state in
                if state == .cancelled {
                    c.resume(throwing: CancellationError())
                } else {
                    c.resume(with: Result(catching: task))
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

final class SQLExecuteBuilder: SQLQueryFetcher {
    let query: any SQLExpression
    let database: any SQLDatabase

    init(query: SQLQueryString, database: any SQLDatabase) {
        self.query = query
        self.database = database
    }
}

extension SQLQueryFetcher {
    func all<T, C0>(collecting: @escaping (C0) -> T) async throws -> [T] where C0: Decodable {
        let all = try await self.all()
        return try all.map { row in
            let allColumns = row.allColumns
            guard allColumns.count >= 1 else {
                throw InternalError("insufficient columns. count: \(allColumns.count)")
            }
            let c0 = try row.decode(column: allColumns[0], as: C0.self)
            return collecting(c0)
        }
    }
    
    func all<T, C0>(collecting: @escaping (C0) -> T) -> EventLoopFuture<[T]> where C0: Decodable {
        return self.all().flatMapEachThrowing { row in
            let allColumns = row.allColumns
            guard allColumns.count >= 1 else {
                throw InternalError("insufficient columns. count: \(allColumns.count)")
            }
            let c0 = try row.decode(column: allColumns[0], as: C0.self)
            return collecting(c0)
        }
    }
    
    func first<T, C0>(collecting: @escaping (C0) -> T) async throws -> T? where C0: Decodable {
        let first = try await self.first()
        return try first.map { row in
            let allColumns = row.allColumns
            guard allColumns.count >= 1 else {
                throw InternalError("insufficient columns. count: \(allColumns.count)")
            }
            let c0 = try row.decode(column: allColumns[0], as: C0.self)
            return collecting(c0)
        }
    }
    
    func first<T, C0>(collecting: @escaping (C0) -> T) -> EventLoopFuture<T?> where C0: Decodable {
        return self.first().flatMapThrowing { row in
            guard let row else { return nil }
            let allColumns = row.allColumns
            guard allColumns.count >= 1 else {
                throw InternalError("insufficient columns. count: \(allColumns.count)")
            }
            let c0 = try row.decode(column: allColumns[0], as: C0.self)
            return collecting(c0)
        }
    }
}

extension Response {
    static func json(status: HTTPResponseStatus = .ok, content: some Encodable) throws -> Response {
        let response = Response(status: status)
        try response.content.encode(content, as: .json)
        return response
    }
}

struct UncheckedBox<T>: @unchecked Sendable {
    var value: T
}

extension AbsolutePath: @unchecked Sendable {}
extension RelativePath: @unchecked Sendable {}

extension SQLiteConnection {
    func transaction<T>(
        _ closure: @escaping (SQLiteConnection) async throws -> T
    ) async throws -> T {
        let closure = UncheckedBox(value: closure)
        return try await withConnection { conn in
            let conn = UncheckedBox(value: conn)
            return conn.value.query("BEGIN TRANSACTION").flatMapWithEventLoop { _, eventLoop in
                eventLoop.performWithTask {
                    try await closure.value(conn.value)
                }.flatMap { result in
                    let result = UncheckedBox(value: result)
                    return conn.value.query("COMMIT TRANSACTION").map { _ in
                        result.value
                    }
                }.flatMapError { error in
                    conn.value.query("ROLLBACK TRANSACTION").flatMapThrowing { _ in
                        throw error
                    }
                }
            }
        }.get()
    }
    
    func transaction<T>(
        _ closure: @escaping (SQLiteConnection) -> EventLoopFuture<T>
    ) -> EventLoopFuture<T> {
        return withConnection { conn in
            return conn.query("BEGIN TRANSACTION").flatMap { _ in
                return closure(conn).flatMap { result in
                    return conn.query("COMMIT TRANSACTION").map { _ in
                        result
                    }
                }.flatMapError { error in
                    conn.query("ROLLBACK TRANSACTION").flatMapThrowing { _ in
                        throw error
                    }
                }
            }
        }
    }
}

extension EventLoop {
    func flatSubmit<T>(_ task: @escaping () throws -> EventLoopFuture<T>) -> EventLoopFuture<T> {
        submit(task).flatMap { $0 }
    }
}
