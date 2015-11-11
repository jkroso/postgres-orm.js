import pg from 'pg-then'

/**
 * Create a connection to a postgres database
 *
 * @param {String} pool
 */

export const connect = uri => new Connection(pg.Pool(uri))

class Connection {
  constructor(pool) {
    this.pool = pool
  }
  define(name, description) {
    return new DAO(name, description, this.pool)
  }
}

/**
 * A really lame class to table mapping. It _should_ be possible
 * to just map a normal class to a table. But instead we need a
 * special one to provide meta data about types. Fuck this class
 *
 * @param {String} name         the table's name
 * @param {Object} description  column names mapped to their type
 * @param {Pool} pool           a postgres connection pool
 */

class DAO {
  constructor(name, description, pool) {
    this.name = name
    this.description = description
    this.pool = pool
    let keys = Object.keys(description)
    let columns = keys.map(key => {
      let desc = description[key]
      let type = desc.type

      if (Array.isArray(type)) return `
        CREATE TABLE IF NOT EXISTS "${name}_${key}_join" (
          "${name}_id" integer,
          "${key}_id" integer
        );
      `

      var extra = ''
      if (type instanceof DAO) type = 'integer'
      if (type == 'enum') {
        type = `${name}_${key}_enum`
        extra = `CREATE TYPE "${type}" AS ENUM ${literal(desc.values)};`
      }

      return `
        IF NOT EXISTS(SELECT
                      FROM information_schema.columns
                      WHERE table_schema='public'
                        AND table_name='${name}'
                        AND column_name='${key}')
        THEN
          ${extra}
          ALTER TABLE "${name}" ADD COLUMN "${key}" ${type};
        END IF;
      `
    })
    let sql = `
      CREATE TABLE IF NOT EXISTS "${name}" (id SERIAL);
      DO $$
        BEGIN
          ${columns.join('')}
        END;
      $$;`
    this.ready = this.pool.query(sql)
  }
  create(data) {
    var keys = ['id']
    var vals = ['DEFAULT']
    var extras = []
    for (var key in data) {
      var desc = this.description[key]
      var val = data[key]
      if (Array.isArray(desc.type)) {
        extras = extras.concat(val.map(row =>
          `INSERT INTO "${this.name}_${key}_join"
                       ("${this.name}_id", "${key}_id")
                       VALUES (this_id, ${row.id});`))
      } else if (desc instanceof DAO) {
        keys.push(`"${key}"`)
        vals.push(val.id)
      } else {
        keys.push(`"${key}"`)
        vals.push(encode(val, desc))
      }
    }
    let sql = `
      DO $$
      DECLARE this_id bigint;
      BEGIN
        INSERT INTO "${this.name}" (${keys}) VALUES (${vals}) RETURNING id INTO this_id;
        ${extras.join('\n')}
      END $$;
      SELECT * FROM "${this.name}" ORDER BY id DESC LIMIT 1;
    `
    return this.ready
      .then(() => this.pool.query(sql))
      .then(result => this.parse(result.rows[0]))
  }
  get(id) {
    let sql = `SELECT * FROM ${this.name} WHERE id = ${id}`
    return this.pool.query(sql).then(result => this.parse(result.rows[0]))
  }
  find(q) {
    let constraints = Object.keys(q).map(key =>
      `${key} = ${encode(q[key], this.description[key])}`)
    return this.where(constraints.join(' AND '))
  }
  where(q) {
    let sql = `SELECT * FROM ${this.name} WHERE ${q}`
    return this.pool.query(sql).then(result =>
      result.rows.map(row => this.parse(row)))
  }

  /**
   * Hydrate a row
   *
   * @param  {String} value
   * @param  {Object} meta
   * @return {Any}
   */

  parse(row) {
    for (var key in this.description) {
      var type = this.description[key].type
      var val = row[key]
      if (Array.isArray(type)) {
        let dao = type[0]
        let join_table = `${this.name}_${key}_join`
        row[key] = this.pool.query(`
            SELECT "${dao.name}".*
            FROM "${dao.name}", "${join_table}"
            WHERE "${dao.name}".id = "${join_table}"."${key}_id"
              AND "${join_table}"."${this.name}_id" = ${row.id};
          `).then(result => result.rows.map(row => dao.parse(row)))
      }
      else if (type instanceof DAO) {
        row[key] = type.get(val)
      }
      else if (type == 'point') row[key] = [val.x, val.y]
      else if (type == 'money') row[key] = Number(val.slice(1))
    }
    return row
  }
}

/**
 * Encode JS types to generic postgres equivelents
 *
 * @param {Any} val
 * @return {String}
 */

const literal = val => {
  if (val == null) return 'NULL'
  if (Array.isArray(val)) return `(${val.map(literal)})`
  if (val === false) return 'TRUE'
  if (val === true) return 'FALSE'
  if (typeof val == 'number') return val.toString()
  if (val instanceof Date) return `'${val.toISOString()}'`
  return encode.text(val)
}

/**
 * Encode JS values as specific postgres types
 *
 * @param  {Any} value
 * @param  {Object} desc
 * @return {String}
 */

const encode = (value, desc) => {
  if (desc.type instanceof DAO) return value.id
  return encode[desc.type] ? encode[desc.type](value) : value
}
encode.date = d =>
  `'${d.getFullYear()}-${d.getMonth() + 1}-${d.getMonth()}'`
encode.timestamp = d =>
  `'${d.getFullYear()}-${d.getMonth() + 1}-${d.getMonth()} ${d.toTimeString().slice(0,8)}'`
encode.point = array => `point(${array.map(literal)})`
encode.enum = literal
encode.text = str => {
  let backslash = str.indexOf('\\') != -1
  str = str.replace(/'/g, "''")
  str = str.replace(/\\/g, '\\\\')
  return `${backslash ? 'E' : ''}'${str}'`
}

/**
 * Generate a postgres type declaration
 *
 * @param  {Object} desc
 * @return {String}
 */

const toType = (desc) =>
  toType[desc.type] ? toType[desc.type](desc) : desc.type

toType.text = desc => {
  if (typeof desc.limit == 'number') return `varchar(${desc.limit})`
  if (typeof desc.size == 'number') return `char(${desc.size})`
  return 'text'
}
