import { ObjectLiteral } from "../../common/ObjectLiteral"
import { TypeORMError } from "../../error"
import { QueryFailedError } from "../../error/QueryFailedError"
import { QueryRunnerAlreadyReleasedError } from "../../error/QueryRunnerAlreadyReleasedError"
import { TransactionNotStartedError } from "../../error/TransactionNotStartedError"
import { ReadStream } from "../../platform/PlatformTools"
import { BaseQueryRunner } from "../../query-runner/BaseQueryRunner"
import { QueryResult } from "../../query-runner/QueryResult"
import { QueryRunner } from "../../query-runner/QueryRunner"
import { TableIndexOptions } from "../../schema-builder/options/TableIndexOptions"
import { Table } from "../../schema-builder/table/Table"
import { TableCheck } from "../../schema-builder/table/TableCheck"
import { TableColumn } from "../../schema-builder/table/TableColumn"
import { TableExclusion } from "../../schema-builder/table/TableExclusion"
import { TableForeignKey } from "../../schema-builder/table/TableForeignKey"
import { TableIndex } from "../../schema-builder/table/TableIndex"
import { TableUnique } from "../../schema-builder/table/TableUnique"
import { View } from "../../schema-builder/view/View"
import { Broadcaster } from "../../subscriber/Broadcaster"
import { BroadcasterResult } from "../../subscriber/BroadcasterResult"
import { InstanceChecker } from "../../util/InstanceChecker"
import { OrmUtils } from "../../util/OrmUtils"
import { VersionUtils } from "../../util/VersionUtils"
import { DriverUtils } from "../DriverUtils"
import { Query } from "../Query"
import { ColumnType } from "../types/ColumnTypes"
import { IsolationLevel } from "../types/IsolationLevel"
import { MetadataTableType } from "../types/MetadataTableType"
import { ReplicationMode } from "../types/ReplicationMode"
import { PostgresJsDriver } from "./PostgresJsDriver"

export class PostgresJsQueryRunner extends BaseQueryRunner implements QueryRunner {
    driver: PostgresJsDriver
    private sqlFn: any
    private txNestingIds: string[] = []
    private savepointCounter = 0

    constructor(driverRef: PostgresJsDriver, replicationMode: ReplicationMode) {
        super()
        this.driver = driverRef
        this.connection = driverRef.connection
        this.mode = replicationMode
        this.broadcaster = new Broadcaster(this)
    }

    async connect(): Promise<any> {
        if (this.sqlFn) return this.sqlFn
        
        this.sqlFn = this.mode === "slave" && this.driver.isReplicated
            ? await this.driver.obtainSlaveConnection()
            : await this.driver.obtainMasterConnection()
        
        this.driver.connectedQueryRunners.push(this)
        return this.sqlFn
    }

    async release(): Promise<void> {
        if (this.isReleased) return
        this.isReleased = true
        this.sqlFn = null
        this.txNestingIds = []
        this.savepointCounter = 0
        const idx = this.driver.connectedQueryRunners.indexOf(this)
        if (idx >= 0) this.driver.connectedQueryRunners.splice(idx, 1)
    }

    async startTransaction(isolation?: IsolationLevel): Promise<void> {
        this.isTransactionActive = true
        try {
            await this.broadcaster.broadcast("BeforeTransactionStart")
        } catch (e) {
            this.isTransactionActive = false
            throw e
        }

        if (this.transactionDepth === 0) {
            await this.performQuery("BEGIN")
            if (isolation) await this.performQuery(`SET TRANSACTION ISOLATION LEVEL ${isolation}`)
        } else {
            const spId = `nest_${this.transactionDepth}_${++this.savepointCounter}`
            this.txNestingIds.push(spId)
            await this.performQuery(`SAVEPOINT "${spId}"`)
        }
        this.transactionDepth++
        await this.broadcaster.broadcast("AfterTransactionStart")
    }

    async commitTransaction(): Promise<void> {
        if (!this.isTransactionActive) throw new TransactionNotStartedError()
        await this.broadcaster.broadcast("BeforeTransactionCommit")

        if (this.transactionDepth === 1) {
            await this.performQuery("COMMIT")
            this.isTransactionActive = false
        } else {
            const spId = this.txNestingIds.pop()
            if (spId) await this.performQuery(`RELEASE SAVEPOINT "${spId}"`)
        }
        this.transactionDepth--
        await this.broadcaster.broadcast("AfterTransactionCommit")
    }

    async rollbackTransaction(): Promise<void> {
        if (!this.isTransactionActive) throw new TransactionNotStartedError()
        await this.broadcaster.broadcast("BeforeTransactionRollback")

        if (this.transactionDepth === 1) {
            await this.performQuery("ROLLBACK")
            this.isTransactionActive = false
        } else {
            const spId = this.txNestingIds.pop()
            if (spId) await this.performQuery(`ROLLBACK TO SAVEPOINT "${spId}"`)
        }
        this.transactionDepth--
        await this.broadcaster.broadcast("AfterTransactionRollback")
    }

    async query(sql: string, params?: any[], structured: boolean = false): Promise<any> {
        if (this.isReleased) throw new QueryRunnerAlreadyReleasedError()
        
        const fn = await this.connect()
        this.driver.connection.logger.logQuery(sql, params, this)
        await this.broadcaster.broadcast("BeforeQuery", sql, params)

        const bcResult = new BroadcasterResult()
        const t0 = ++this.savepointCounter

        try {
            const result = params ? await fn.unsafe(sql, params) : await fn.unsafe(sql)
            const elapsed = ++this.savepointCounter - t0
            
            this.broadcaster.broadcastAfterQueryEvent(bcResult, sql, params, true, elapsed, result, undefined)
            
            const maxTime = this.driver.options.maxQueryExecutionTime
            if (maxTime && elapsed > maxTime) {
                this.driver.connection.logger.logQuerySlow(elapsed, sql, params, this)
            }

            return structured ? this.wrapInStructure(result) : this.normalizeOutput(result)
        } catch (err) {
            this.driver.connection.logger.logQueryError(err, sql, params, this)
            this.broadcaster.broadcastAfterQueryEvent(bcResult, sql, params, false, undefined, undefined, err)
            throw new QueryFailedError(sql, params, err)
        } finally {
            await bcResult.wait()
        }
    }

    async stream(sql: string, params?: any[], onEnd?: Function, onError?: Function): Promise<ReadStream> {
        throw new TypeORMError("Streaming not supported by postgres.js - use cursor-based pagination")
    }

    async getDatabases(): Promise<string[]> { return [] }
    async getSchemas(db?: string): Promise<string[]> { return [] }

    async hasDatabase(name: string): Promise<boolean> {
        const r = await this.performQuery("SELECT datname FROM pg_database WHERE datname = $1", [name])
        return r.length > 0
    }

    async getCurrentDatabase(): Promise<string> {
        const r = await this.performQuery("SELECT current_database()")
        return r[0].current_database
    }

    async hasSchema(name: string): Promise<boolean> {
        const r = await this.performQuery(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name = $1",
            [name]
        )
        return r.length > 0
    }

    async getCurrentSchema(): Promise<string> {
        const r = await this.performQuery("SELECT current_schema()")
        return r[0].current_schema
    }

    async hasTable(tbl: Table | string): Promise<boolean> {
        const meta = this.driver.parseTableName(tbl)
        const sch = meta.schema || await this.getCurrentSchema()
        const r = await this.performQuery(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2",
            [sch, meta.tableName]
        )
        return r.length > 0
    }

    async hasColumn(tbl: Table | string, col: string): Promise<boolean> {
        const meta = this.driver.parseTableName(tbl)
        const sch = meta.schema || await this.getCurrentSchema()
        const r = await this.performQuery(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 AND column_name = $3",
            [sch, meta.tableName, col]
        )
        return r.length > 0
    }

    async createDatabase(name: string, ifNotExist?: boolean): Promise<void> {
        if (ifNotExist && await this.hasDatabase(name)) return
        await this.executeQueries(
            new Query(`CREATE DATABASE "${name}"`),
            new Query(`DROP DATABASE "${name}"`)
        )
    }

    async dropDatabase(name: string, ifExist?: boolean): Promise<void> {
        const sql = ifExist ? `DROP DATABASE IF EXISTS "${name}"` : `DROP DATABASE "${name}"`
        await this.executeQueries(
            new Query(sql),
            new Query(`CREATE DATABASE "${name}"`)
        )
    }

    async createSchema(path: string, ifNotExist?: boolean): Promise<void> {
        const name = path.indexOf(".") === -1 ? path : path.split(".")[0]
        const sql = ifNotExist ? `CREATE SCHEMA IF NOT EXISTS "${name}"` : `CREATE SCHEMA "${name}"`
        await this.executeQueries(
            new Query(sql),
            new Query(`DROP SCHEMA "${name}" CASCADE`)
        )
    }

    async dropSchema(path: string, ifExist?: boolean, cascade?: boolean): Promise<void> {
        const name = path.indexOf(".") === -1 ? path : path.split(".")[0]
        const cascStr = cascade ? " CASCADE" : ""
        const existStr = ifExist ? " IF EXISTS" : ""
        await this.executeQueries(
            new Query(`DROP SCHEMA${existStr} "${name}"${cascStr}`),
            new Query(`CREATE SCHEMA "${name}"`)
        )
    }

    async createTable(tbl: Table, ifNotExist?: boolean, createFks?: boolean, createIdx?: boolean): Promise<void> {
        if (ifNotExist && await this.hasTable(tbl)) return
        
        const ups: Query[] = []
        const downs: Query[] = []
        
        ups.push(this.makeTableCreationSql(tbl))
        downs.push(new Query(`DROP TABLE ${this.quotePath(tbl)}`))
        
        if (createFks !== false && tbl.foreignKeys.length > 0) {
            tbl.foreignKeys.forEach(fk => {
                ups.push(this.makeFkAdditionSql(tbl, fk))
                downs.push(this.makeFkRemovalSql(tbl, fk))
            })
        }
        
        if (createIdx !== false && tbl.indices.length > 0) {
            tbl.indices.forEach(idx => {
                ups.push(this.makeIndexCreationSql(tbl, idx))
                downs.push(this.makeIndexRemovalSql(idx))
            })
        }
        
        await this.executeQueries(ups, downs)
    }

    async dropTable(tbl: Table | string, ifExist?: boolean, dropFks?: boolean, dropIdx?: boolean): Promise<void> {
        const path = this.getTablePath(tbl)
        const existStr = ifExist ? " IF EXISTS" : ""
        await this.executeQueries(
            new Query(`DROP TABLE${existStr} ${path} CASCADE`),
            new Query(`CREATE TABLE ${path} ()`)
        )
    }

    async createView(v: View, sync?: boolean, oldV?: View, rev?: boolean): Promise<void> {
        const matStr = v.materialized ? "MATERIALIZED " : ""
        await this.executeQueries(
            new Query(`CREATE ${matStr}VIEW ${this.quotePath(v)} AS ${v.expression}`),
            new Query(`DROP ${matStr}VIEW ${this.quotePath(v)}`)
        )
    }

    async dropView(v: View | string): Promise<void> {
        const path = this.getTablePath(v)
        await this.executeQueries(
            new Query(`DROP VIEW ${path}`),
            new Query(`CREATE VIEW ${path} AS SELECT 1`)
        )
    }

    async renameTable(old: Table | string, neu: Table | string): Promise<void> {
        const oldPath = this.getTablePath(old)
        const neuPath = this.getTablePath(neu)
        await this.executeQueries(
            new Query(`ALTER TABLE ${oldPath} RENAME TO ${neuPath}`),
            new Query(`ALTER TABLE ${neuPath} RENAME TO ${oldPath}`)
        )
    }

    async addColumn(tbl: Table | string, col: TableColumn): Promise<void> {
        const path = this.getTablePath(tbl)
        const def = this.makeColumnDef(col)
        await this.executeQueries(
            new Query(`ALTER TABLE ${path} ADD COLUMN ${def}`),
            new Query(`ALTER TABLE ${path} DROP COLUMN "${col.name}"`)
        )
    }

    async addColumns(tbl: Table | string, cols: TableColumn[]): Promise<void> {
        for (const c of cols) await this.addColumn(tbl, c)
    }

    async renameColumn(tbl: Table | string, old: TableColumn | string, neu: TableColumn | string): Promise<void> {
        const path = this.getTablePath(tbl)
        const oldN = typeof old === "string" ? old : old.name
        const neuN = typeof neu === "string" ? neu : neu.name
        await this.executeQueries(
            new Query(`ALTER TABLE ${path} RENAME COLUMN "${oldN}" TO "${neuN}"`),
            new Query(`ALTER TABLE ${path} RENAME COLUMN "${neuN}" TO "${oldN}"`)
        )
    }

    async changeColumn(tbl: Table | string, old: TableColumn, neu: TableColumn): Promise<void> {
        const path = this.getTablePath(tbl)
        const ups: Query[] = []
        const downs: Query[] = []
        
        if (old.type !== neu.type) {
            ups.push(new Query(`ALTER TABLE ${path} ALTER COLUMN "${neu.name}" TYPE ${neu.type}`))
            downs.push(new Query(`ALTER TABLE ${path} ALTER COLUMN "${old.name}" TYPE ${old.type}`))
        }
        
        if (old.isNullable !== neu.isNullable) {
            const nullMod = neu.isNullable ? "DROP NOT NULL" : "SET NOT NULL"
            ups.push(new Query(`ALTER TABLE ${path} ALTER COLUMN "${neu.name}" ${nullMod}`))
        }
        
        await this.executeQueries(ups, downs)
    }

    async changeColumns(tbl: Table | string, changes: {oldColumn: TableColumn, newColumn: TableColumn}[]): Promise<void> {
        for (const {oldColumn, newColumn} of changes) {
            await this.changeColumn(tbl, oldColumn, newColumn)
        }
    }

    async dropColumn(tbl: Table | string, col: TableColumn | string): Promise<void> {
        const path = this.getTablePath(tbl)
        const name = typeof col === "string" ? col : col.name
        await this.executeQueries(
            new Query(`ALTER TABLE ${path} DROP COLUMN "${name}"`),
            new Query(`ALTER TABLE ${path} ADD COLUMN "${name}" integer`)
        )
    }

    async dropColumns(tbl: Table | string, cols: (TableColumn | string)[]): Promise<void> {
        for (const c of cols) await this.dropColumn(tbl, c)
    }

    async createPrimaryKey(tbl: Table | string, cols: string[], name?: string): Promise<void> {
        const path = this.getTablePath(tbl)
        const pkName = name || this.connection.namingStrategy.primaryKeyName(tbl, cols)
        const colList = cols.map(n => `"${n}"`).join(", ")
        await this.executeQueries(
            new Query(`ALTER TABLE ${path} ADD CONSTRAINT "${pkName}" PRIMARY KEY (${colList})`),
            new Query(`ALTER TABLE ${path} DROP CONSTRAINT "${pkName}"`)
        )
    }

    async dropPrimaryKey(tbl: Table | string): Promise<void> {
        const path = this.getTablePath(tbl)
        const t = typeof tbl === "string" ? await this.getCachedTable(tbl) : tbl
        const pkName = t.primaryColumns[0]?.primaryKeyConstraintName
        if (!pkName) throw new TypeORMError(`PK constraint name not found for ${path}`)
        await this.executeQueries(
            new Query(`ALTER TABLE ${path} DROP CONSTRAINT "${pkName}"`),
            new Query(`ALTER TABLE ${path} ADD CONSTRAINT "${pkName}" PRIMARY KEY ()`)
        )
    }

    async createUniqueConstraint(tbl: Table | string, uniq: TableUnique): Promise<void> {
        const path = this.getTablePath(tbl)
        const colList = uniq.columnNames.map(n => `"${n}"`).join(", ")
        await this.executeQueries(
            new Query(`ALTER TABLE ${path} ADD CONSTRAINT "${uniq.name}" UNIQUE (${colList})`),
            new Query(`ALTER TABLE ${path} DROP CONSTRAINT "${uniq.name}"`)
        )
    }

    async createUniqueConstraints(tbl: Table | string, uniqs: TableUnique[]): Promise<void> {
        for (const u of uniqs) await this.createUniqueConstraint(tbl, u)
    }

    async dropUniqueConstraint(tbl: Table | string, uniq: TableUnique | string): Promise<void> {
        const path = this.getTablePath(tbl)
        const name = typeof uniq === "string" ? uniq : uniq.name
        await this.executeQueries(
            new Query(`ALTER TABLE ${path} DROP CONSTRAINT "${name}"`),
            new Query(`ALTER TABLE ${path} ADD CONSTRAINT "${name}" UNIQUE ()`)
        )
    }

    async dropUniqueConstraints(tbl: Table | string, uniqs: TableUnique[]): Promise<void> {
        for (const u of uniqs) await this.dropUniqueConstraint(tbl, u)
    }

    async createCheckConstraint(tbl: Table | string, chk: TableCheck): Promise<void> {
        const path = this.getTablePath(tbl)
        await this.executeQueries(
            new Query(`ALTER TABLE ${path} ADD CONSTRAINT "${chk.name}" CHECK (${chk.expression})`),
            new Query(`ALTER TABLE ${path} DROP CONSTRAINT "${chk.name}"`)
        )
    }

    async createCheckConstraints(tbl: Table | string, chks: TableCheck[]): Promise<void> {
        for (const c of chks) await this.createCheckConstraint(tbl, c)
    }

    async dropCheckConstraint(tbl: Table | string, chk: TableCheck | string): Promise<void> {
        const path = this.getTablePath(tbl)
        const name = typeof chk === "string" ? chk : chk.name
        await this.executeQueries(
            new Query(`ALTER TABLE ${path} DROP CONSTRAINT "${name}"`),
            new Query(`ALTER TABLE ${path} ADD CONSTRAINT "${name}" CHECK (true)`)
        )
    }

    async dropCheckConstraints(tbl: Table | string, chks: TableCheck[]): Promise<void> {
        for (const c of chks) await this.dropCheckConstraint(tbl, c)
    }

    async createExclusionConstraint(tbl: Table | string, excl: TableExclusion): Promise<void> {
        const path = this.getTablePath(tbl)
        await this.executeQueries(
            new Query(`ALTER TABLE ${path} ADD CONSTRAINT "${excl.name}" EXCLUDE ${excl.expression}`),
            new Query(`ALTER TABLE ${path} DROP CONSTRAINT "${excl.name}"`)
        )
    }

    async createExclusionConstraints(tbl: Table | string, excls: TableExclusion[]): Promise<void> {
        for (const e of excls) await this.createExclusionConstraint(tbl, e)
    }

    async dropExclusionConstraint(tbl: Table | string, excl: TableExclusion | string): Promise<void> {
        const path = this.getTablePath(tbl)
        const name = typeof excl === "string" ? excl : excl.name
        await this.executeQueries(
            new Query(`ALTER TABLE ${path} DROP CONSTRAINT "${name}"`),
            new Query(`ALTER TABLE ${path} ADD CONSTRAINT "${name}" EXCLUDE USING gist (id WITH =)`)
        )
    }

    async dropExclusionConstraints(tbl: Table | string, excls: TableExclusion[]): Promise<void> {
        for (const e of excls) await this.dropExclusionConstraint(tbl, e)
    }

    async createForeignKey(tbl: Table | string, fk: TableForeignKey): Promise<void> {
        await this.executeQueries(
            this.makeFkAdditionSql(tbl, fk),
            this.makeFkRemovalSql(tbl, fk)
        )
    }

    async createForeignKeys(tbl: Table | string, fks: TableForeignKey[]): Promise<void> {
        for (const f of fks) await this.createForeignKey(tbl, f)
    }

    async dropForeignKey(tbl: Table | string, fk: TableForeignKey | string): Promise<void> {
        const path = this.getTablePath(tbl)
        const name = typeof fk === "string" ? fk : fk.name
        await this.executeQueries(
            new Query(`ALTER TABLE ${path} DROP CONSTRAINT "${name}"`),
            new Query(`ALTER TABLE ${path} ADD CONSTRAINT "${name}" FOREIGN KEY () REFERENCES dummy()`)
        )
    }

    async dropForeignKeys(tbl: Table | string, fks: TableForeignKey[]): Promise<void> {
        for (const f of fks) await this.dropForeignKey(tbl, f)
    }

    async createIndex(tbl: Table | string, idx: TableIndex): Promise<void> {
        await this.executeQueries(
            this.makeIndexCreationSql(tbl, idx),
            this.makeIndexRemovalSql(idx)
        )
    }

    async createIndices(tbl: Table | string, idxs: TableIndex[]): Promise<void> {
        for (const i of idxs) await this.createIndex(tbl, i)
    }

    async dropIndex(tbl: Table | string, idx: TableIndex | string): Promise<void> {
        const name = typeof idx === "string" ? idx : idx.name
        await this.executeQueries(
            new Query(`DROP INDEX "${name}"`),
            new Query(`CREATE INDEX "${name}" ON dummy (id)`)
        )
    }

    async dropIndices(tbl: Table | string, idxs: TableIndex[]): Promise<void> {
        for (const i of idxs) await this.dropIndex(tbl, i)
    }

    async clearTable(name: string): Promise<void> {
        await this.performQuery(`TRUNCATE TABLE ${this.quotePath(name)}`)
    }

    protected async loadViews(names?: string[]): Promise<View[]> {
        const sch = await this.getCurrentSchema()
        const filter = names ? names.map(n => {
            const p = this.driver.parseTableName(n)
            return `(t.schema = '${p.schema || sch}' AND t.name = '${p.tableName}')`
        }).join(" OR ") : ""
        
        const metaTbl = this.getTypeormMetadataTableName()
        const where = filter ? ` AND (${filter})` : ""
        
        const sql = `
            SELECT t.* FROM ${metaTbl} t
            INNER JOIN pg_catalog.pg_class c ON c.relname = t.name
            INNER JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.schema
            WHERE t.type IN ('${MetadataTableType.VIEW}', '${MetadataTableType.MATERIALIZED_VIEW}')${where}
        `
        
        const rows = await this.performQuery(sql)
        return rows.map((r: any) => {
            const v = new View()
            v.schema = r.schema
            v.name = this.driver.buildTableName(r.name, r.schema)
            v.expression = r.value
            v.materialized = r.type === MetadataTableType.MATERIALIZED_VIEW
            return v
        })
    }

    protected async loadTables(names?: string[]): Promise<Table[]> {
        if (names && names.length === 0) return []
        
        const sch = await this.getCurrentSchema()
        const db = await this.getCurrentDatabase()
        
        let filter = ""
        if (names) {
            filter = " WHERE " + names.map(n => {
                const p = this.driver.parseTableName(n)
                return `(table_schema = '${p.schema || sch}' AND table_name = '${p.tableName}')`
            }).join(" OR ")
        }
        
        const sql = `SELECT table_schema, table_name FROM information_schema.tables${filter}`
        const rows = await this.performQuery(sql)
        
        if (rows.length === 0) return []
        
        return rows.map((r: any) => {
            const t = new Table()
            t.database = db
            t.schema = r.table_schema
            t.name = this.driver.buildTableName(r.table_name, r.table_schema)
            return t
        })
    }

    async getVersion(): Promise<string> {
        const r = await this.performQuery("SELECT version()")
        return r[0].version
    }

    private quotePath(target: Table | View | string): string {
        const p = this.driver.parseTableName(target)
        return p.schema ? `"${p.schema}"."${p.tableName}"` : `"${p.tableName}"`
    }

    private async performQuery(sql: string, params?: any[]): Promise<any> {
        return await this.query(sql, params)
    }

    private normalizeOutput(raw: any): any {
        if (!raw) return raw
        if (raw.command) {
            switch (raw.command) {
                case "DELETE":
                case "UPDATE":
                    return [raw, raw.count]
                default:
                    return raw
            }
        }
        return raw
    }

    private wrapInStructure(raw: any): QueryResult {
        const res = new QueryResult()
        if (Array.isArray(raw)) {
            res.records = raw
            res.raw = raw
        } else if (raw.command) {
            res.records = raw
            res.affected = raw.count
            res.raw = raw
        }
        return res
    }

    private makeColumnDef(col: TableColumn): string {
        let def = `"${col.name}" ${col.type}`
        if (col.length) def += `(${col.length})`
        if (col.isNullable !== true) def += " NOT NULL"
        if (col.default !== undefined) def += ` DEFAULT ${col.default}`
        if (col.isGenerated && col.generationStrategy === "increment") def = `"${col.name}" SERIAL`
        return def
    }

    private makeTableCreationSql(tbl: Table): Query {
        const cols = tbl.columns.map(c => this.makeColumnDef(c))
        
        if (tbl.primaryColumns.length > 0) {
            const pkCols = tbl.primaryColumns.map(c => `"${c.name}"`).join(", ")
            const pkName = tbl.primaryColumns[0].primaryKeyConstraintName ||
                this.connection.namingStrategy.primaryKeyName(tbl, tbl.primaryColumns.map(c => c.name))
            cols.push(`CONSTRAINT "${pkName}" PRIMARY KEY (${pkCols})`)
        }
        
        return new Query(`CREATE TABLE ${this.quotePath(tbl)} (${cols.join(", ")})`)
    }

    private makeIndexCreationSql(tbl: Table | string, idx: TableIndex): Query {
        const path = this.getTablePath(tbl)
        const cols = idx.columnNames.map(n => `"${n}"`).join(", ")
        const uniq = idx.isUnique ? "UNIQUE " : ""
        const where = idx.where ? ` WHERE ${idx.where}` : ""
        return new Query(`CREATE ${uniq}INDEX "${idx.name}" ON ${path} (${cols})${where}`)
    }

    private makeIndexRemovalSql(idx: TableIndex): Query {
        return new Query(`DROP INDEX "${idx.name}"`)
    }

    private makeFkAdditionSql(tbl: Table | string, fk: TableForeignKey): Query {
        const path = this.getTablePath(tbl)
        const cols = fk.columnNames.map(n => `"${n}"`).join(", ")
        const refCols = fk.referencedColumnNames.map(n => `"${n}"`).join(", ")
        const refPath = this.getTablePath(fk.referencedTableName)
        
        let sql = `ALTER TABLE ${path} ADD CONSTRAINT "${fk.name}" FOREIGN KEY (${cols}) REFERENCES ${refPath} (${refCols})`
        if (fk.onDelete) sql += ` ON DELETE ${fk.onDelete}`
        if (fk.onUpdate) sql += ` ON UPDATE ${fk.onUpdate}`
        
        return new Query(sql)
    }

    private makeFkRemovalSql(tbl: Table | string, fk: TableForeignKey): Query {
        const path = this.getTablePath(tbl)
        return new Query(`ALTER TABLE ${path} DROP CONSTRAINT "${fk.name}"`)
    }
}
