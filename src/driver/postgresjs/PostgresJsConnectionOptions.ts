import { PostgresConnectionOptions } from "../postgres/PostgresConnectionOptions"

/**
 * Postgres.js-specific connection options.
 */
export interface PostgresJsConnectionOptions
    extends Omit<
        PostgresConnectionOptions,
        "type" | "driver" | "nativeDriver"
    > {
    /**
     * Database type.
     */
    readonly type: "postgresjs"

    /**
     * The driver object.
     * This defaults to `require("postgres")`.
     */
    readonly driver?: any
}
