import { Icon } from '@/ui/Icon'

export interface Selectable {
  selected: boolean;
}

export namespace Default {
  export type Object =
    | 'OPERATION'
    | 'CREATE_OPERATION'
    | 'CONTEXT'
    | 'SOURCE'
    | 'NOTE'
    | 'LINK'
    | 'EVENT'
    | 'SESSION'
    | 'HIGHLIGHT'

  export const Icon: Record<Object, Icon.Name> = {
    OPERATION: 'BookDashed',
    CREATE_OPERATION: 'BookPlus',
    CONTEXT: 'Box',
    EVENT: 'Triangle',
    SOURCE: 'File',
    NOTE: 'StickyNote',
    LINK: 'Link',
    SESSION: 'FacePlus',
    HIGHLIGHT: 'Status'
  }

  export const Color: Record<Object, string> = {
    OPERATION: '#3399ff',
    CREATE_OPERATION: '#3399ff',
    CONTEXT: '#65b58b',
    SOURCE: '#c99900',
    NOTE: '#009999',
    LINK: '#c99900',
    EVENT: '#ff408c',
    SESSION: '#ff4d4d',
    HIGHLIGHT: 'blue',
  }
}

export type Version = `${number}.${number}.${number}`;