import { expect } from "chai"

import { DataSource } from "../../../src/data-source/DataSource"
import { DriverFactory } from "../../../src/driver/DriverFactory"
import { DriverUtils } from "../../../src/driver/DriverUtils"
import { PostgresJsDriver } from "../../../src/driver/postgresjs/PostgresJsDriver"

describe("PostgresJsDriver", () => {
    it("creates a postgres.js driver via DriverFactory", () => {
        const dataSource = new DataSource({
            type: "postgresjs",
            database: "test",
            host: "localhost",
            username: "user",
            password: "password",
        })
        const driver = new DriverFactory().create(dataSource)

        expect(driver).to.be.instanceOf(PostgresJsDriver)
        expect(driver.options.type).to.equal("postgresjs")
    })

    it("is treated as a Postgres family driver", () => {
        const dataSource = new DataSource({
            type: "postgresjs",
            database: "test",
        })
        const driver = new DriverFactory().create(dataSource)

        expect(DriverUtils.isPostgresFamily(driver)).to.equal(true)
    })
})
