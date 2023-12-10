const { Pool } = require("pg");

export const pool = new Pool({
  host: "databases",
  database: "db_processing",
  user: "uach",
  password: "uachbienestar2023",
  port: 5432,
});
