import { expect } from "chai"
import { PostgresJsUtils } from "../../../src/driver/postgresjs/PostgresJsUtils"

describe("PostgresJsUtils", () => {
    describe("buildSqlFunctionConfig", () => {
        it("should build config with URL credentials", () => {
            const credentials = {
                url: "postgres://user:pass@localhost:5432/testdb",
            }
            const globalOptions = {}

            const config = PostgresJsUtils.buildSqlFunctionConfig(
                credentials,
                globalOptions,
            )

            expect(config.connection).to.equal(
                "postgres://user:pass@localhost:5432/testdb",
            )
        })

        it("should build config with individual connection parameters", () => {
            const credentials = {
                host: "localhost",
                port: 5432,
                username: "testuser",
                database: "testdb",
                password: "testpass",
            }
            const globalOptions = {}

            const config = PostgresJsUtils.buildSqlFunctionConfig(
                credentials,
                globalOptions,
            )

            expect(config.host).to.equal("localhost")
            expect(config.port).to.equal(5432)
            expect(config.user).to.equal("testuser")
            expect(config.database).to.equal("testdb")
            expect(config.pass).to.equal("testpass")
        })

        it("should build config with SSL configuration", () => {
            const credentials = {
                host: "localhost",
                ssl: {
                    rejectUnauthorized: false,
                },
            }
            const globalOptions = {}

            const config = PostgresJsUtils.buildSqlFunctionConfig(
                credentials,
                globalOptions,
            )

            expect(config.ssl).to.deep.equal({ rejectUnauthorized: false })
        })

        it("should build config with pool configuration options", () => {
            const credentials = {
                host: "localhost",
            }
            const globalOptions = {
                max: 10,
                idleTimeout: 30000,
                maxLifetime: 60000,
                connectTimeoutMS: 5000,
            }

            const config = PostgresJsUtils.buildSqlFunctionConfig(
                credentials,
                globalOptions,
            )

            expect(config.max).to.equal(10)
            expect(config.idle_timeout).to.equal(30000)
            expect(config.max_lifetime).to.equal(60000)
            expect(config.connect_timeout).to.equal(5)
        })

        it("should build config with postgres.js specific features", () => {
            const credentials = {
                host: "localhost",
            }
            const globalOptions = {
                prepare: true,
                transform: { undefined: null },
                debug: true,
            }

            const config = PostgresJsUtils.buildSqlFunctionConfig(
                credentials,
                globalOptions,
            )

            expect(config.prepare).to.equal(true)
            expect(config.transform).to.deep.equal({ undefined: null })
            expect(config.debug).to.equal(true)
        })

        it("should handle password as string", () => {
            const credentials = {
                host: "localhost",
                password: "mypassword",
            }
            const globalOptions = {}

            const config = PostgresJsUtils.buildSqlFunctionConfig(
                credentials,
                globalOptions,
            )

            expect(config.pass).to.equal("mypassword")
        })

        it("should handle password as function", () => {
            const passwordFn = () => "dynamic-password"
            const credentials = {
                host: "localhost",
                password: passwordFn,
            }
            const globalOptions = {}

            const config = PostgresJsUtils.buildSqlFunctionConfig(
                credentials,
                globalOptions,
            )

            expect(config.pass).to.equal(passwordFn)
        })

        it("should build config with all options combined", () => {
            const credentials = {
                host: "localhost",
                port: 5432,
                username: "testuser",
                database: "testdb",
                password: "testpass",
                ssl: true,
            }
            const globalOptions = {
                max: 20,
                idleTimeout: 10000,
                prepare: false,
            }

            const config = PostgresJsUtils.buildSqlFunctionConfig(
                credentials,
                globalOptions,
            )

            expect(config.host).to.equal("localhost")
            expect(config.port).to.equal(5432)
            expect(config.user).to.equal("testuser")
            expect(config.database).to.equal("testdb")
            expect(config.pass).to.equal("testpass")
            expect(config.ssl).to.equal(true)
            expect(config.max).to.equal(20)
            expect(config.idle_timeout).to.equal(10000)
            expect(config.prepare).to.equal(false)
        })

        it("should not set undefined values in config", () => {
            const credentials = {}
            const globalOptions = {}

            const config = PostgresJsUtils.buildSqlFunctionConfig(
                credentials,
                globalOptions,
            )

            expect(config.host).to.be.undefined
            expect(config.port).to.be.undefined
            expect(config.user).to.be.undefined
            expect(config.database).to.be.undefined
            expect(config.pass).to.be.undefined
            expect(config.ssl).to.be.undefined
        })
    })

    describe("convertParameters", () => {
        it("should convert positional parameters correctly", () => {
            const sql = "SELECT * FROM users WHERE id = $1 AND name = $2"
            const params = [123, "John"]

            const [convertedSql, convertedParams] =
                PostgresJsUtils.convertParameters(sql, params)

            expect(convertedSql).to.equal(
                "SELECT * FROM users WHERE id = $1 AND name = $2",
            )
            expect(convertedParams).to.deep.equal([123, "John"])
        })

        it("should handle parameters in different order", () => {
            const sql = "SELECT * FROM users WHERE name = $2 AND id = $1"
            const params = [123, "John"]

            const [convertedSql, convertedParams] =
                PostgresJsUtils.convertParameters(sql, params)

            // convertParameters processes parameters sequentially as they appear in SQL
            expect(convertedSql).to.equal(
                "SELECT * FROM users WHERE name = $1 AND id = $2",
            )
            expect(convertedParams).to.deep.equal(["John", 123])
        })

        it("should handle missing parameters", () => {
            const sql = "SELECT * FROM users WHERE id = $1 AND name = $3"
            const params = [123]

            const [convertedSql, convertedParams] =
                PostgresJsUtils.convertParameters(sql, params)

            expect(convertedSql).to.equal(
                "SELECT * FROM users WHERE id = $1 AND name = $3",
            )
            expect(convertedParams).to.deep.equal([123])
        })

        it("should handle empty parameters", () => {
            const sql = "SELECT * FROM users"
            const params: any[] = []

            const [convertedSql, convertedParams] =
                PostgresJsUtils.convertParameters(sql, params)

            expect(convertedSql).to.equal("SELECT * FROM users")
            expect(convertedParams).to.deep.equal([])
        })

        it("should handle multiple instances of same parameter", () => {
            const sql = "SELECT * FROM users WHERE id = $1 OR parent_id = $1"
            const params = [123]

            const [convertedSql, convertedParams] =
                PostgresJsUtils.convertParameters(sql, params)

            expect(convertedSql).to.equal(
                "SELECT * FROM users WHERE id = $1 OR parent_id = $2",
            )
            expect(convertedParams).to.deep.equal([123, 123])
        })

        it("should handle complex queries with many parameters", () => {
            const sql =
                "INSERT INTO users (id, name, email, age) VALUES ($1, $2, $3, $4)"
            const params = [1, "Alice", "alice@example.com", 30]

            const [convertedSql, convertedParams] =
                PostgresJsUtils.convertParameters(sql, params)

            expect(convertedSql).to.equal(
                "INSERT INTO users (id, name, email, age) VALUES ($1, $2, $3, $4)",
            )
            expect(convertedParams).to.deep.equal([
                1,
                "Alice",
                "alice@example.com",
                30,
            ])
        })

        it("should handle null and undefined parameters", () => {
            const sql = "SELECT * FROM users WHERE name = $1 AND age = $2"
            const params = [null, undefined]

            const [convertedSql, convertedParams] =
                PostgresJsUtils.convertParameters(sql, params)

            expect(convertedSql).to.equal(
                "SELECT * FROM users WHERE name = $1 AND age = $2",
            )
            // Note: convertParameters only includes parameters that are not undefined
            expect(convertedParams).to.deep.equal([null])
        })
    })

    describe("extractResult", () => {
        it("should extract result from array response", () => {
            const pgJsResult = [
                { id: 1, name: "Alice" },
                { id: 2, name: "Bob" },
            ]

            const result = PostgresJsUtils.extractResult(pgJsResult)

            expect(result.rows).to.deep.equal(pgJsResult)
            expect(result.rowCount).to.equal(2)
        })

        it("should extract result from array with command property", () => {
            const pgJsResult = [{ id: 1, name: "Alice" }] as any
            pgJsResult.command = "SELECT"

            const result = PostgresJsUtils.extractResult(pgJsResult)

            expect(result.rows).to.deep.equal(pgJsResult)
            expect(result.rowCount).to.equal(1)
            expect(result.command).to.equal("SELECT")
        })

        it("should handle empty array result", () => {
            const pgJsResult: any[] = []

            const result = PostgresJsUtils.extractResult(pgJsResult)

            expect(result.rows).to.deep.equal([])
            expect(result.rowCount).to.equal(0)
        })

        it("should return non-array result as-is", () => {
            const pgJsResult = {
                rows: [{ id: 1 }],
                rowCount: 1,
                command: "SELECT",
            }

            const result = PostgresJsUtils.extractResult(pgJsResult)

            expect(result).to.deep.equal(pgJsResult)
        })

        it("should handle large result sets", () => {
            const pgJsResult = Array.from({ length: 1000 }, (_, i) => ({
                id: i,
                value: `value${i}`,
            }))

            const result = PostgresJsUtils.extractResult(pgJsResult)

            expect(result.rows).to.have.lengthOf(1000)
            expect(result.rowCount).to.equal(1000)
        })
    })

    describe("formatError", () => {
        it("should format error with all properties", () => {
            const originalError = {
                message: "Query failed",
                code: "23505",
                detail: "Duplicate key value",
                hint: "Check unique constraint",
                position: "10",
                internalPosition: "5",
                internalQuery: "SELECT * FROM users",
                where: "table users",
                schema: "public",
                table: "users",
                column: "email",
                dataType: "varchar",
                constraint: "users_email_key",
                file: "nbtinsert.c",
                line: "123",
                routine: "ExecInsert",
            }
            const query = "INSERT INTO users (email) VALUES ($1)"
            const parameters = ["test@example.com"]

            const formattedError = PostgresJsUtils.formatError(
                originalError,
                query,
                parameters,
            )

            expect(formattedError.message).to.equal("Query failed")
            expect((formattedError as any).query).to.equal(query)
            expect((formattedError as any).parameters).to.deep.equal(parameters)
            expect((formattedError as any).code).to.equal("23505")
            expect((formattedError as any).detail).to.equal(
                "Duplicate key value",
            )
            expect((formattedError as any).hint).to.equal(
                "Check unique constraint",
            )
            expect((formattedError as any).position).to.equal("10")
            expect((formattedError as any).internalPosition).to.equal("5")
            expect((formattedError as any).internalQuery).to.equal(
                "SELECT * FROM users",
            )
            expect((formattedError as any).where).to.equal("table users")
            expect((formattedError as any).schema).to.equal("public")
            expect((formattedError as any).table).to.equal("users")
            expect((formattedError as any).column).to.equal("email")
            expect((formattedError as any).dataType).to.equal("varchar")
            expect((formattedError as any).constraint).to.equal(
                "users_email_key",
            )
            expect((formattedError as any).file).to.equal("nbtinsert.c")
            expect((formattedError as any).line).to.equal("123")
            expect((formattedError as any).routine).to.equal("ExecInsert")
        })

        it("should format error with minimal properties", () => {
            const originalError = {
                message: "Connection failed",
            }
            const query = "SELECT 1"
            const parameters: any[] = []

            const formattedError = PostgresJsUtils.formatError(
                originalError,
                query,
                parameters,
            )

            expect(formattedError.message).to.equal("Connection failed")
            expect((formattedError as any).query).to.equal(query)
            expect((formattedError as any).parameters).to.deep.equal([])
            expect((formattedError as any).code).to.be.undefined
            expect((formattedError as any).detail).to.be.undefined
        })

        it("should handle empty error message", () => {
            const originalError = {
                message: "",
                code: "08006",
            }
            const query = "SELECT * FROM users"
            const parameters = [1]

            const formattedError = PostgresJsUtils.formatError(
                originalError,
                query,
                parameters,
            )

            expect(formattedError.message).to.equal("")
            expect((formattedError as any).code).to.equal("08006")
        })

        it("should preserve error properties even if falsy", () => {
            const originalError = {
                message: "Error",
                code: "0",
                position: "0",
                line: "0",
            }
            const query = "UPDATE users SET active = $1"
            const parameters = [false]

            const formattedError = PostgresJsUtils.formatError(
                originalError,
                query,
                parameters,
            )

            expect((formattedError as any).code).to.equal("0")
            expect((formattedError as any).position).to.equal("0")
            expect((formattedError as any).line).to.equal("0")
        })

        it("should return an Error instance", () => {
            const originalError = {
                message: "Test error",
            }
            const query = "SELECT 1"
            const parameters: any[] = []

            const formattedError = PostgresJsUtils.formatError(
                originalError,
                query,
                parameters,
            )

            expect(formattedError).to.be.instanceOf(Error)
        })
    })
})
