const { Pool } = require("pg");

export const pool = new Pool({
  host: "databases",
  database: "db_processing",
  user: "root",
  password: "root",
  port: 5432
});
