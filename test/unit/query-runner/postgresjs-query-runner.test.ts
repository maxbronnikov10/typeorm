import { expect } from "chai"
import * as sinon from "sinon"
import { PostgresJsQueryRunner } from "../../../src/driver/postgresjs/PostgresJsQueryRunner"

describe("PostgresJsQueryRunner", () => {
    let queryRunner: PostgresJsQueryRunner
    let driver: any
    let dataSource: any
    let performQueryStub: sinon.SinonStub

    beforeEach(() => {
        // Create a minimal mock DataSource
        dataSource = {
            options: { type: "postgres" },
            driver: {} as any,
        } as any

        // Create a minimal mock driver
        driver = {
            connection: dataSource,
            isReplicated: false,
            connectedQueryRunners: [],
            obtainMasterConnection: sinon.stub().resolves({}),
            obtainSlaveConnection: sinon.stub().resolves({}),
            parseTableName: sinon.stub().returns({ tableName: "test", schema: "public" }),
            isFullTextColumnTypeSupported: true,
        } as any

        dataSource.driver = driver

        // Create query runner
        queryRunner = new PostgresJsQueryRunner(driver, "master")

        // Stub the performQuery and broadcast methods
        performQueryStub = sinon.stub(queryRunner as any, "performQuery").resolves()
        sinon.stub((queryRunner as any).broadcaster, "broadcast").resolves()
    })

    afterEach(() => {
        sinon.restore()
    })

    describe("savepoint counter functionality", () => {
        it("should generate unique savepoint names for nested transactions", async () => {
            // Start first transaction (no savepoint)
            await queryRunner.startTransaction()
            expect(performQueryStub.callCount).to.equal(1)
            expect(performQueryStub.getCall(0).args[0]).to.equal("BEGIN")

            // Start nested transaction (first savepoint)
            await queryRunner.startTransaction()
            expect(performQueryStub.callCount).to.equal(2)
            const firstSavepoint = performQueryStub.getCall(1).args[0]
            expect(firstSavepoint).to.match(/^SAVEPOINT "nest_1_1"$/)

            // Start another nested transaction (second savepoint)
            await queryRunner.startTransaction()
            expect(performQueryStub.callCount).to.equal(3)
            const secondSavepoint = performQueryStub.getCall(2).args[0]
            expect(secondSavepoint).to.match(/^SAVEPOINT "nest_2_2"$/)

            // Commit nested transactions
            await queryRunner.commitTransaction()
            expect(performQueryStub.getCall(3).args[0]).to.match(
                /^RELEASE SAVEPOINT "nest_2_2"$/,
            )

            await queryRunner.commitTransaction()
            expect(performQueryStub.getCall(4).args[0]).to.match(
                /^RELEASE SAVEPOINT "nest_1_1"$/,
            )

            await queryRunner.commitTransaction()
            expect(performQueryStub.getCall(5).args[0]).to.equal("COMMIT")
        })

        it("should reset savepoint counter on release", async () => {
            // Start and commit a transaction with savepoint
            await queryRunner.startTransaction()
            await queryRunner.startTransaction()
            const firstSavepoint = performQueryStub.getCall(1).args[0]
            expect(firstSavepoint).to.match(/^SAVEPOINT "nest_1_1"$/)

            await queryRunner.commitTransaction()
            await queryRunner.commitTransaction()

            // Release the query runner
            await queryRunner.release()

            // Create a new query runner
            queryRunner = new PostgresJsQueryRunner(driver, "master")
            performQueryStub = sinon
                .stub(queryRunner as any, "performQuery")
                .resolves()
            sinon
                .stub((queryRunner as any).broadcaster, "broadcast")
                .resolves()

            // Start transactions again - counter should start from 1
            await queryRunner.startTransaction()
            await queryRunner.startTransaction()
            const newSavepoint = performQueryStub.getCall(1).args[0]
            expect(newSavepoint).to.match(/^SAVEPOINT "nest_1_1"$/)
        })

        it("should generate sequential savepoint names for multiple nested transactions at same level", async () => {
            // Start outer transaction
            await queryRunner.startTransaction()

            // Start and commit first nested transaction
            await queryRunner.startTransaction()
            const firstNested = performQueryStub.getCall(1).args[0]
            expect(firstNested).to.match(/^SAVEPOINT "nest_1_1"$/)
            await queryRunner.commitTransaction()

            // Start and commit second nested transaction
            await queryRunner.startTransaction()
            const secondNested = performQueryStub.getCall(3).args[0]
            expect(secondNested).to.match(/^SAVEPOINT "nest_1_2"$/)
            await queryRunner.commitTransaction()

            // Start and commit third nested transaction
            await queryRunner.startTransaction()
            const thirdNested = performQueryStub.getCall(5).args[0]
            expect(thirdNested).to.match(/^SAVEPOINT "nest_1_3"$/)
            await queryRunner.commitTransaction()

            await queryRunner.commitTransaction()
        })

        it("should handle rollback with correct savepoint names", async () => {
            // Start outer transaction
            await queryRunner.startTransaction()

            // Start nested transaction
            await queryRunner.startTransaction()
            const savepoint = performQueryStub.getCall(1).args[0]
            expect(savepoint).to.match(/^SAVEPOINT "nest_1_1"$/)

            // Rollback nested transaction
            await queryRunner.rollbackTransaction()
            const rollback = performQueryStub.getCall(2).args[0]
            expect(rollback).to.match(/^ROLLBACK TO SAVEPOINT "nest_1_1"$/)

            await queryRunner.commitTransaction()
        })

        it("should maintain correct savepoint counter across complex nesting", async () => {
            // Level 0: Start outer transaction
            await queryRunner.startTransaction()
            expect(performQueryStub.getCall(0).args[0]).to.equal("BEGIN")

            // Level 1: First nested
            await queryRunner.startTransaction()
            expect(performQueryStub.getCall(1).args[0]).to.match(
                /^SAVEPOINT "nest_1_1"$/,
            )

            // Level 2: Deeply nested
            await queryRunner.startTransaction()
            expect(performQueryStub.getCall(2).args[0]).to.match(
                /^SAVEPOINT "nest_2_2"$/,
            )

            // Commit level 2
            await queryRunner.commitTransaction()

            // Commit level 1
            await queryRunner.commitTransaction()

            // Level 1: Second nested (counter should continue)
            await queryRunner.startTransaction()
            expect(performQueryStub.getCall(5).args[0]).to.match(
                /^SAVEPOINT "nest_1_3"$/,
            )

            // Commit level 1
            await queryRunner.commitTransaction()

            // Commit level 0
            await queryRunner.commitTransaction()
        })
    })

    describe("transaction depth management", () => {
        it("should properly track transaction depth", async () => {
            expect((queryRunner as any).transactionDepth).to.equal(0)

            await queryRunner.startTransaction()
            expect((queryRunner as any).transactionDepth).to.equal(1)

            await queryRunner.startTransaction()
            expect((queryRunner as any).transactionDepth).to.equal(2)

            await queryRunner.commitTransaction()
            expect((queryRunner as any).transactionDepth).to.equal(1)

            await queryRunner.commitTransaction()
            expect((queryRunner as any).transactionDepth).to.equal(0)
        })

        it("should reset transaction depth on rollback", async () => {
            await queryRunner.startTransaction()
            await queryRunner.startTransaction()
            expect((queryRunner as any).transactionDepth).to.equal(2)

            await queryRunner.rollbackTransaction()
            expect((queryRunner as any).transactionDepth).to.equal(1)

            await queryRunner.rollbackTransaction()
            expect((queryRunner as any).transactionDepth).to.equal(0)
        })
    })

    describe("nesting ID stack management", () => {
        it("should maintain stack of nesting IDs", async () => {
            await queryRunner.startTransaction()
            expect((queryRunner as any).txNestingIds).to.have.lengthOf(0)

            await queryRunner.startTransaction()
            expect((queryRunner as any).txNestingIds).to.have.lengthOf(1)
            expect((queryRunner as any).txNestingIds[0]).to.equal("nest_1_1")

            await queryRunner.startTransaction()
            expect((queryRunner as any).txNestingIds).to.have.lengthOf(2)
            expect((queryRunner as any).txNestingIds[1]).to.equal("nest_2_2")

            await queryRunner.commitTransaction()
            expect((queryRunner as any).txNestingIds).to.have.lengthOf(1)

            await queryRunner.commitTransaction()
            expect((queryRunner as any).txNestingIds).to.have.lengthOf(0)

            await queryRunner.commitTransaction()
        })

        it("should clear nesting ID stack on release", async () => {
            await queryRunner.startTransaction()
            await queryRunner.startTransaction()
            expect((queryRunner as any).txNestingIds).to.have.lengthOf(1)

            await queryRunner.release()
            expect((queryRunner as any).txNestingIds).to.have.lengthOf(0)
        })
    })
})
