# PostgresJS Driver for TypeORM

Performance-optimized PostgreSQL driver using the `postgres` npm package (postgres.js).

## Key Features

- **Promise-Native**: Built on postgres.js's async/await API
- **Auto-Pooling**: Connection management handled automatically  
- **Performance**: Optimized query execution with prepared statements
- **Compatibility**: Full PostgreSQL feature support

## Differences from Standard postgres Driver

| Feature | Standard (pg) | PostgresJS |
|---------|--------------|------------|
| API Style | Callback-based | Promise-native |
| Pooling | Manual pool management | Automatic |
| Query Interface | callback/promise | Tagged templates + unsafe() |
| Performance | Standard | Optimized |

## Usage

```typescript
import { DataSource } from "typeorm"

const dataSource = new DataSource({
    type: "postgresjs",
    host: "localhost",
    port: 5432,
    username: "user",
    password: "password",
    database: "mydb",
    entities: [/*...*/]
})
```

## Implementation Notes

This driver uses the standard PostgreSQL wire protocol but with postgres.js's 
optimized implementation. It shares column type mappings and SQL dialect with 
the standard postgres driver while providing performance improvements.
