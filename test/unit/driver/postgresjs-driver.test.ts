import { expect } from "chai"
import * as sinon from "sinon"
import { PostgresJsDriver } from "../../../src/driver/postgresjs/PostgresJsDriver"
import { DataSource } from "../../../src/data-source/DataSource"

describe("PostgresJsDriver", () => {
    let driver: PostgresJsDriver
    let dataSource: DataSource

    beforeEach(() => {
        // Create a minimal mock DataSource
        dataSource = {
            options: {
                type: "postgresjs",
                host: "localhost",
                database: "testdb",
            },
        } as any

        driver = new PostgresJsDriver()
        // Manually set properties to avoid initialization issues
        driver.connection = dataSource
        driver.options = dataSource.options as any
        driver.database = "testdb"
        driver.schema = "public"
    })

    afterEach(() => {
        sinon.restore()
    })

    describe("escape", () => {
        it("should escape column names with double quotes", () => {
            const escaped = driver.escape("columnName")
            expect(escaped).to.equal('"columnName"')
        })

        it("should escape special column names", () => {
            const escaped = driver.escape("user")
            expect(escaped).to.equal('"user"')
        })

        it("should escape column names with spaces", () => {
            const escaped = driver.escape("column name")
            expect(escaped).to.equal('"column name"')
        })
    })

    describe("parseTableName", () => {
        it("should parse simple table name", () => {
            const parsed = driver.parseTableName("users")

            expect(parsed.tableName).to.equal("users")
            expect(parsed.schema).to.equal("public")
            expect(parsed.database).to.equal("testdb")
        })

        it("should parse table name with schema", () => {
            const parsed = driver.parseTableName("myschema.users")

            expect(parsed.tableName).to.equal("users")
            expect(parsed.schema).to.equal("myschema")
            expect(parsed.database).to.equal("testdb")
        })

        it("should parse table name without schema when driver has no default schema", () => {
            driver.schema = undefined
            const parsed = driver.parseTableName("users")

            expect(parsed.tableName).to.equal("users")
            expect(parsed.schema).to.be.undefined
            expect(parsed.database).to.equal("testdb")
        })

        it("should parse table name with schema even when driver has different default", () => {
            driver.schema = "public"
            const parsed = driver.parseTableName("custom.users")

            expect(parsed.tableName).to.equal("users")
            expect(parsed.schema).to.equal("custom")
        })

        it("should handle table name without database", () => {
            driver.database = undefined
            const parsed = driver.parseTableName("users")

            expect(parsed.tableName).to.equal("users")
            expect(parsed.database).to.be.undefined
        })
    })

    describe("buildTableName", () => {
        it("should build simple table name", () => {
            const tableName = driver.buildTableName("users")
            expect(tableName).to.equal('"users"')
        })

        it("should build table name with schema", () => {
            const tableName = driver.buildTableName("users", "myschema")
            expect(tableName).to.equal('"myschema"."users"')
        })

        it("should use driver default schema when not provided", () => {
            driver.schema = "custom"
            const tableName = driver.buildTableName("users")
            expect(tableName).to.equal('"custom"."users"')
        })

        it("should not prefix with public schema", () => {
            driver.schema = "public"
            const tableName = driver.buildTableName("users")
            expect(tableName).to.equal('"users"')
        })

        it("should override driver schema with provided schema", () => {
            driver.schema = "public"
            const tableName = driver.buildTableName("users", "custom")
            expect(tableName).to.equal('"custom"."users"')
        })

        it("should use searchSchema when schema is not public", () => {
            driver.schema = undefined
            driver.searchSchema = "search"
            const tableName = driver.buildTableName("users")
            expect(tableName).to.equal('"search"."users"')
        })

        it("should not use searchSchema when it is public", () => {
            driver.schema = undefined
            driver.searchSchema = "public"
            const tableName = driver.buildTableName("users")
            expect(tableName).to.equal('"users"')
        })
    })

    describe("normalizeType", () => {
        it("should normalize Number type", () => {
            const normalized = driver.normalizeType({ type: Number })
            expect(normalized).to.equal("integer")
        })

        it("should normalize integer type", () => {
            const normalized = driver.normalizeType({ type: "integer" })
            expect(normalized).to.equal("integer")
        })

        it("should normalize String type", () => {
            const normalized = driver.normalizeType({ type: String })
            expect(normalized).to.equal("character varying")
        })

        it("should normalize Date type", () => {
            const normalized = driver.normalizeType({ type: Date })
            expect(normalized).to.equal("timestamp without time zone")
        })

        it("should normalize Boolean type", () => {
            const normalized = driver.normalizeType({ type: Boolean })
            expect(normalized).to.equal("boolean")
        })

        it("should normalize Buffer type", () => {
            const normalized = driver.normalizeType({ type: Buffer as any })
            expect(normalized).to.equal("bytea")
        })

        it("should return custom type as-is", () => {
            const normalized = driver.normalizeType({ type: "jsonb" })
            expect(normalized).to.equal("jsonb")
        })

        it("should return empty string for undefined type", () => {
            const normalized = driver.normalizeType({})
            expect(normalized).to.equal("")
        })

        it("should handle varchar type", () => {
            const normalized = driver.normalizeType({ type: "varchar" })
            expect(normalized).to.equal("varchar")
        })

        it("should handle text type", () => {
            const normalized = driver.normalizeType({ type: "text" })
            expect(normalized).to.equal("text")
        })
    })

    describe("isFullTextColumnTypeSupported", () => {
        it("should return false as postgres.js does not support full text search natively", () => {
            expect(driver.isFullTextColumnTypeSupported()).to.equal(false)
        })
    })
})
