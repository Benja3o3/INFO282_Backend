const { Pool } = require("pg");

export const pool = new Pool({
  user: "postgres",
  host: "localhost",
  database: "db_processing",
  password: "benja123",
});
