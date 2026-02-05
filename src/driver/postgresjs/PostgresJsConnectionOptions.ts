import { BaseDataSourceOptions } from "../../data-source/BaseDataSourceOptions"
import { ReplicationMode } from "../types/ReplicationMode"
import { PostgresJsConnectionCredentialsOptions } from "./PostgresJsConnectionCredentialsOptions"

export interface PostgresJsConnectionOptions
    extends BaseDataSourceOptions,
        PostgresJsConnectionCredentialsOptions {
    readonly type: "postgresjs"
    readonly schema?: string
    readonly driver?: any
    readonly useUTC?: boolean
    readonly replication?: {
        readonly master: PostgresJsConnectionCredentialsOptions
        readonly slaves: PostgresJsConnectionCredentialsOptions[]
        readonly defaultMode?: ReplicationMode
    }
    readonly connectTimeoutMS?: number
    readonly uuidExtension?: "pgcrypto" | "uuid-ossp"
    readonly poolErrorHandler?: (err: any) => any
    readonly logNotifications?: boolean
    readonly installExtensions?: boolean
    readonly parseInt8?: boolean
    readonly extensions?: string[]
    readonly max?: number
    readonly idleTimeout?: number
    readonly maxLifetime?: number
    readonly prepare?: boolean
    readonly transform?: {
        column?: (column: string) => string
        row?: (row: any) => any
        value?: (value: any) => any
    }
    readonly debug?:
        | boolean
        | ((
              connection: number,
              query: string,
              parameters: any[],
              types: any[],
          ) => void)
}
