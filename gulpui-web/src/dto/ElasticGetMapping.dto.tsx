export type Acceptable = 'date_nanos' | 'long' | 'text' | 'keyword' | 'ip'

export interface ElasticGetMappingUnit {
  [key: string]: Acceptable
}
