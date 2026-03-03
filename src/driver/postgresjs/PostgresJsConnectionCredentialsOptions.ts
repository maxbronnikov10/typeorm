import { TlsOptions } from "tls"

export interface PostgresJsConnectionCredentialsOptions {
    url?: string
    host?: string  
    port?: number
    username?: string
    password?: string | (() => string) | (() => Promise<string>)
    database?: string
    ssl?: boolean | TlsOptions
    applicationName?: string
}
