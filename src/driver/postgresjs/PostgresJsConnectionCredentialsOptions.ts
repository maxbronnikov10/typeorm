import { TlsOptions } from "tls"

export interface PostgresJsConnectionCredentialsOptions {
    readonly url?: string
    readonly host?: string
    readonly port?: number
    readonly username?: string
    readonly password?: string | (() => string) | (() => Promise<string>)
    readonly database?: string
    readonly ssl?: boolean | TlsOptions
    readonly applicationName?: string
}
