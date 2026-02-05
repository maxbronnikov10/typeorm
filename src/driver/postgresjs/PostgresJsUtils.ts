/**
 * Helper utilities specific to postgres.js driver implementation.
 * Provides abstractions for postgres.js's unique function-based API.
 */
export class PostgresJsUtils {
    /**
     * Build configuration object for postgres.js sql() function.
     * postgres.js uses a single function call, not a Pool class.
     */
    static buildSqlFunctionConfig(
        credentials: any,
        globalOptions: any,
    ): any {
        const config: any = {}

        // Connection parameters
        if (credentials.url) config.connection = credentials.url
        if (credentials.host) config.host = credentials.host
        if (credentials.port) config.port = credentials.port
        if (credentials.username) config.user = credentials.username
        if (credentials.database) config.database = credentials.database
        if (credentials.ssl !== undefined) config.ssl = credentials.ssl

        // Pool configuration (postgres.js handles internally)
        if (globalOptions.max) config.max = globalOptions.max
        if (globalOptions.idleTimeout) config.idle_timeout = globalOptions.idleTimeout
        if (globalOptions.maxLifetime) config.max_lifetime = globalOptions.maxLifetime
        if (globalOptions.connectTimeoutMS)
            config.connect_timeout = Math.floor(globalOptions.connectTimeoutMS / 1000)

        // postgres.js specific features
        if (globalOptions.prepare !== undefined) config.prepare = globalOptions.prepare
        if (globalOptions.transform) config.transform = globalOptions.transform
        if (globalOptions.debug) config.debug = globalOptions.debug

        // Handle password (can be function or string)
        if (credentials.password) {
            if (typeof credentials.password === "function") {
                config.pass = credentials.password
            } else {
                config.pass = credentials.password
            }
        }

        return config
    }

    /**
     * Convert TypeORM parameter format to postgres.js format.
     * postgres.js uses positional parameters ($1, $2, etc).
     */
    static convertParameters(sql: string, params: any): [string, any[]] {
        const paramArray: any[] = []
        let index = 1

        const converted = sql.replace(/\$(\d+)/g, (match, num) => {
            const paramIndex = parseInt(num) - 1
            if (params[paramIndex] !== undefined) {
                paramArray.push(params[paramIndex])
                return `$${index++}`
            }
            return match
        })

        return [converted, paramArray]
    }

    /**
     * Extract rows from postgres.js result.
     * postgres.js returns array directly, not { rows: [] } object.
     */
    static extractResult(pgJsResult: any): any {
        // postgres.js returns array directly
        if (Array.isArray(pgJsResult)) {
            return {
                rows: pgJsResult,
                rowCount: pgJsResult.length,
                command: pgJsResult.command,
            }
        }
        return pgJsResult
    }

    /**
     * Format error from postgres.js to TypeORM format.
     */
    static formatError(error: any, query: string, parameters: any[]): Error {
        const err = new Error(error.message)
        ;(err as any).query = query
        ;(err as any).parameters = parameters
        ;(err as any).code = error.code
        ;(err as any).detail = error.detail
        ;(err as any).hint = error.hint
        ;(err as any).position = error.position
        ;(err as any).internalPosition = error.internalPosition
        ;(err as any).internalQuery = error.internalQuery
        ;(err as any).where = error.where
        ;(err as any).schema = error.schema
        ;(err as any).table = error.table
        ;(err as any).column = error.column
        ;(err as any).dataType = error.dataType
        ;(err as any).constraint = error.constraint
        ;(err as any).file = error.file
        ;(err as any).line = error.line
        ;(err as any).routine = error.routine
        return err
    }
}
