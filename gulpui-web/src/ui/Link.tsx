import { Application } from '@/context/Application.context'
import { DisplayGroupDialog } from '@/dialogs/Group.dialog'
import { Point } from './Point'
import { Link } from '@/entities/Link'
import { Doc } from '@/entities/Doc'
import { DisplayEventDialog } from '@/dialogs/Event.dialog'

export namespace LinkPoint {
  export interface Props extends Omit<Point.Props, 'icon' | 'name' | 'accent'> {
    link: Link.Type
  }
}

export function LinkPoint({ link, ...props }: LinkPoint.Props) {
  const { app, spawnDialog } = Application.use()

  /**
   * Opens the event or linked-events group represented by this link point.
   *
   * @returns Nothing when the link has no connected documents.
   */
  const openEvent = () => {
    if (link.doc_ids.length === 0) {
      return null
    }

    const dialog =
      link.doc_ids.length === 1 ? (
        <DisplayEventDialog event={Doc.Entity.id(app, link.doc_id_from)} />
      ) : (
        <DisplayGroupDialog
          events={link.doc_ids.map(id => Doc.Entity.id(app, id))}
          groupKind="links"
        />
      )

    spawnDialog(dialog)
  }

  return (
    // @ts-ignore
    <Point
      onClick={openEvent}
      icon={Link.Entity.icon(link)}
      name={link.name}
      accent={link.color}
      {...props}
    />
  )
}
