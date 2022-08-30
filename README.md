# isucon12-qualify-swift

ISUCON12予選問題の、非公式なSwift実装です。

公式: https://github.com/isucon/isucon12-qualify

# 利用方法

このリポジトリの`/webapp`以下のファイルを公式実装の`/webapp`にコピーすることであとは他の言語と同様に利用できます。

## その他

### SQLiteログのダンプ機能は未実装です

あまり競技に影響のない部分であることと、言語ごとに仕様が厳密に揃ってる部分ではないと思ったのでサボりました。
SQLiteの接続にはsqlite-kitを使用しましたが、そちらがデフォルトでログを出力してくれるので、SQLiteの挙動の調査にも特に困らないのではないかと思います。

### Swift実装特有の気をつけたいポイント

#### リクエストボディの最大値

Vaporは普通に利用するとリクエスト処理時にボディを自動で読み込みますが、デフォルトだとその最大値が小さいです。この文章を書いた時点では`16kb`に設定されています。今回の問題においてこれより大きなサイズでPOSTされることがあり、最大値が小さいままだとエラーになってしまいます。

手っ取り早く一律で最大値を変更したければ、以下のように書けます。

```swift
app.routes.defaultMaxBodySize = "1mb"
```

### StackTrace

アプリケーション各所でVaporの`Abort`を用いてエラーを返すようにしていますが、`Abort`は生成時にスタックトレースをキャプチャします。このスタックトレースのキャプチャは比較的重い処理であり、エラーを頻繁に返すような状況であれば気になる負荷かもしれません。

手っ取り早く一律でStackTraceのキャプチャを無効にしたければ、以下のように書けます。

```swift
StackTrace.isCaptureEnabled = false 
```

### EventLoop & Swift concurrency

本実装には[Swift concurreny](https://docs.swift.org/swift-book/LanguageGuide/Concurrency.html)を使用しています。
これまでSwift on Serverにおけるノンブロッキングなイベント駆動処理の実行基盤として、SwiftNIOのEventLoopが広く使用されてきました。Swift concurrencyの実行基盤はその代替になり得るものですが、まだ出たばかりで過渡期という状況です。本実装ではこれらが混ざり合っており、たびたびそのコンテキストスイッチが発生しています。

```swift
// EventLoopからSwift concurrencyのコンテキストへのホップ
let foo = try await eventLoopFuture.get()

// Swift concurrencyからEventLoopへのホップ
eventLoop.performWithTask {
    try await task()
}
```

このホップではスレッドの移動が起こるため、オーバヘッドがあります。場合によっては、このオーバヘッドが気になるタイミングが訪れるかもしれません。

### ログ

ログにはSQLiteのログが大量に流れます。
あまりの量につき、その処理時間がボトルネックと言えてしまいそうなレベルです。

手っ取り早く一律でデバッグログの出力を抑えたければ、以下のように書けます。

```diff
LoggingSystem.bootstrap { label in
    var handler = ConsoleLogger(label: label, console: Terminal())
--    handler.logLevel = .debug
++    handler.logLevel = .error
    return handler
}
```
