import { BaseDataSourceOptions } from "../../data-source/BaseDataSourceOptions"
import { PostgresJsConnectionCredentialsOptions } from "./PostgresJsConnectionCredentialsOptions"

export interface PostgresJsConnectionOptions
    extends BaseDataSourceOptions,
        PostgresJsConnectionCredentialsOptions {
    readonly type: "postgresjs"
    readonly schema?: string
    readonly driver?: any
    readonly useUTC?: boolean
    readonly connectTimeoutMS?: number
    readonly uuidExtension?: "pgcrypto" | "uuid-ossp"
    readonly poolErrorHandler?: (err: any) => any
    readonly logNotifications?: boolean
    readonly installExtensions?: boolean
    readonly parseInt8?: boolean
    readonly extensions?: string[]
    readonly idleTimeoutMillis?: number
    readonly prepare?: boolean
}
