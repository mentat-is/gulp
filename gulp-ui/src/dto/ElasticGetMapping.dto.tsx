/** OpenSearch field types returned by the source-field metadata endpoint. */
export type Acceptable =
  | 'boolean'
  | 'byte'
  | 'date'
  | 'date_nanos'
  | 'double'
  | 'float'
  | 'half_float'
  | 'integer'
  | 'ip'
  | 'keyword'
  | 'long'
  | 'scaled_float'
  | 'short'
  | 'text'
  | 'unsigned_long'
  | 'wildcard'

export interface ElasticGetMappingUnit {
  [key: string]: Acceptable
}
