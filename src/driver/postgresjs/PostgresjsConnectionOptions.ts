import { BaseDataSourceOptions } from "../../data-source/BaseDataSourceOptions"
import { TlsOptions } from "tls"

/**
 * Postgres.js-specific connection options.
 * This driver uses the 'postgres' npm package (postgres.js).
 */
export interface PostgresjsConnectionOptions extends BaseDataSourceOptions {
    /**
     * Database type - must be 'postgresjs'
     */
    readonly type: "postgresjs"

    /**
     * Connection URL (postgres://...)
     */
    readonly url?: string

    /**
     * Database host address(es)
     * Can be a single host or an array for high availability
     */
    readonly host?: string | string[]

    /**
     * Database port(s) - defaults to 5432
     * Can be a number or array matching host array
     */
    readonly port?: number | number[]

    /**
     * Unix socket path (alternative to host/port)
     */
    readonly path?: string

    /**
     * Database name
     */
    readonly database?: string

    /**
     * Database user
     */
    readonly user?: string
    readonly username?: string

    /**
     * Database password
     */
    readonly password?: string | (() => string) | (() => Promise<string>)

    /**
     * Schema name for queries
     */
    readonly schema?: string

    /**
     * SSL configuration
     * Can be 'require', 'allow', 'prefer', 'verify-full', boolean, or TLS options
     */
    readonly ssl?:
        | "require"
        | "allow"
        | "prefer"
        | "verify-full"
        | boolean
        | TlsOptions

    /**
     * Maximum number of connections in pool
     * @default 10
     */
    readonly max?: number

    /**
     * Idle connection timeout in seconds
     */
    readonly idle_timeout?: number

    /**
     * Connection timeout in seconds
     * @default 30
     */
    readonly connect_timeout?: number

    /**
     * Enable prepared statements
     * @default true
     */
    readonly prepare?: boolean

    /**
     * Target session attributes for read-write routing
     */
    readonly target_session_attrs?:
        | "read-write"
        | "read-only"
        | "primary"
        | "standby"
        | "prefer-standby"

    /**
     * Automatically fetch custom types from database
     * @default true
     */
    readonly fetch_types?: boolean

    /**
     * Debug mode - logs all queries with connection id
     */
    readonly debug?:
        | boolean
        | ((
              connection: number,
              query: string,
              parameters: any[],
              paramTypes: any[],
          ) => void)

    /**
     * Connection keep-alive duration in seconds
     * Set to null to disable keep-alive
     */
    readonly keep_alive?: number | null

    /**
     * Maximum connection lifetime in seconds
     * Set to null for unlimited lifetime
     */
    readonly max_lifetime?: number | null

    /**
     * Transform configuration for postgres.js
     */
    readonly transform?: {
        /**
         * Transform undefined values in queries
         */
        undefined?: any

        /**
         * Transform column names in results and queries
         */
        column?:
            | ((column: string) => string)
            | {
                  from?: (column: string) => string
                  to?: (column: string) => string
              }

        /**
         * Transform values in results
         */
        value?:
            | ((value: any) => any)
            | {
                  from?: (value: any, column: any) => any
              }

        /**
         * Transform entire rows
         */
        row?:
            | ((row: any) => any)
            | {
                  from?: (row: any) => any
              }
    }

    /**
     * Connection parameters for advanced configuration
     */
    readonly connection?: Record<string, any>

    /**
     * Callback when connection is closed
     */
    readonly onclose?: (connectionId: number) => void

    /**
     * Callback when database sends a notice
     */
    readonly onnotice?: (notice: any) => void

    /**
     * Callback when server parameters change
     */
    readonly onparameter?: (key: string, value: any) => void

    /**
     * Backoff strategy for reconnection attempts
     * Can be boolean or function returning delay in milliseconds
     */
    readonly backoff?: boolean | ((attemptNum: number) => number)

    /**
     * Custom types configuration for postgres.js
     */
    readonly types?: Record<string, any>

    /**
     * The Postgres extension to use for UUID generation
     * @default "uuid-ossp"
     */
    readonly uuidExtension?: "pgcrypto" | "uuid-ossp"

    /**
     * Automatically install postgres extensions
     * @default true
     */
    readonly installExtensions?: boolean

    /**
     * List of additional Postgres extensions to install
     */
    readonly extensions?: string[]

    /**
     * Direct postgres.js driver instance (for advanced usage)
     * Overrides connection configuration
     */
    readonly driver?: any
}
