import { Application } from '@/context/Application.context';
import { Badge } from '@/ui/Badge';
import { Banner as UIBanner } from '@/ui/Banner';
import { Icon } from '@/ui/Icon';
import { cn } from '@/ui/utils';
import type { KeyboardEvent, ReactNode } from 'react';
import { useCallback, useEffect, useMemo, useState } from 'react';
import s from './styles/RequestsBanner.module.css';
import { formatDistanceToNow } from 'date-fns';
import { enUS } from 'date-fns/locale';
import { Stack } from '@/ui/Stack';
import { Request } from '@/entities/Request';
import { Button } from '@/ui/Button';
import { Locale } from '@/locales';
import { useRequests } from '@/store/request.store';

export namespace Requests {
  export namespace Banner {
    export type Props = UIBanner.Props
  }

  /**
   * Renders the requests list and details for the selected request.
   * @param props Banner props forwarded to the shared banner component.
   * @returns Requests banner content.
   */
  export function Banner({ className, ...props }: Requests.Banner.Props) {
    const { Info } = Application.use();
    const { t } = Locale.use();
    const requests = useRequests();
    const [loading, setLoading] = useState<boolean>(false);
    const [selectedRequestId, setSelectedRequestId] = useState<Request.Id | null>(null);

    /**
     * Formats a timestamp as a relative time label.
     * @param timestamp Unix timestamp in milliseconds.
     * @returns Human-readable relative time.
     */
    const timeAgo = useCallback((timestamp: number): string => {
      return formatDistanceToNow(timestamp, {
        addSuffix: true,
        locale: enUS,
      })
    }, []);

    /**
     * Sends a cancel request for an active backend request.
     * @param id Backend request identifier.
     * @returns The request-cancel API promise.
     */
    const cancelRequestButtonClickHandler = useCallback(
      (id: Request.Type['id']) => Info.request_cancel(id),
      [Info],
    );

    /**
     * Reloads the request list for the selected operation.
     * @returns A promise that resolves when loading state is reset.
     */
    const reload = useCallback(async () => {
      setLoading(true);
      await Info.request_list();
      setLoading(false);
    }, [Info]);

    /**
     * Selects the request whose details should be shown.
     * @param request Request row selected by the user.
     * @returns Nothing.
     */
    const selectRequest = useCallback((request: Request.Type): void => {
      setSelectedRequestId(request.id);
    }, []);

    /**
     * Handles keyboard activation for request rows.
     * @param event Keyboard event from the focused row.
     * @param request Request represented by the focused row.
     * @returns Nothing.
     */
    const requestRowKeyDownHandler = useCallback((
      event: KeyboardEvent<HTMLDivElement>,
      request: Request.Type,
    ): void => {
      if (event.key !== 'Enter' && event.key !== ' ') {
        return;
      }

      event.preventDefault();
      selectRequest(request);
    }, [selectRequest]);

    const RefreshRequestsListButton = useMemo(() => {
      return <Button variant='secondary' loading={loading} onClick={reload} icon='RefreshCcw'>{t('common.refresh')}</Button>
    }, [reload, loading, t]);

    useEffect(() => {
      reload();
    }, [reload]);

    const sortedRequests = useMemo(() => {
      return [...requests].sort((a, b) => b.time_created - a.time_created);
    }, [requests]);

    const selectedRequest = useMemo(() => {
      return sortedRequests.find((request) => request.id === selectedRequestId) ?? null;
    }, [selectedRequestId, sortedRequests]);

    useEffect(() => {
      if (selectedRequestId && !selectedRequest) {
        setSelectedRequestId(null);
      }
    }, [selectedRequestId, selectedRequest]);

    return (
      <UIBanner className={cn(className, s.banner)} title={t('requests.title')} subtitle={RefreshRequestsListButton} {...props}>
        <Stack dir="column" gap={0} className={s.list}>
          {sortedRequests.map((request) => {
            const isSelected = selectedRequestId === request.id;

            return (
              <Stack
                key={request.id}
                className={cn(s.combination, isSelected && s.selected)}
                role="button"
                tabIndex={0}
                aria-pressed={isSelected}
                onClick={() => selectRequest(request)}
                onKeyDown={(event) => requestRowKeyDownHandler(event, request)}
              >
                <Status status={request.status} />
                <p className={s.id}>{request.id}</p>
                <hr />
                <p className={s.time_ago}>{timeAgo(request.time_created)}</p>
                {!Requests.Status.FinishedStatuses.includes(request.status) && (
                  <Badge
                    className={s.close}
                    variant='red'
                    onClick={(event) => {
                      event.stopPropagation();
                      cancelRequestButtonClickHandler(request.id);
                    }}
                  >
                    <Icon name="X" />
                    {t('common.cancel')}
                  </Badge>
                )}
              </Stack>
            )
          })}
        </Stack>
        {selectedRequest && <RequestDetails request={selectedRequest} />}
      </UIBanner>
    )
  }

  /**
   * Renders a status icon using request status color maps.
   * @param props Status props with the request status.
   * @returns Status icon container.
   */
  export function Status({ status, ...props }: Status.Props) {
    return (
      <Stack
        className={s.status}
        style={{
          background: Requests.Status.BackgroundsMap[status],
          color: Requests.Status.ColorsMap[status],
        }}
        {...props}
      >
        <Icon size={12} name={Requests.Status.IconsMap[status]} />
      </Stack>
    )
  }

  export namespace Status {
    export interface Props extends Stack.Props {
      status: Request.Status
    }

    export const IconsMap: Record<Request.Status, Icon.Name> = {
      done: 'Check',
      canceled: 'X',
      failed: 'X',
      pending: 'StatusSmall',
      ongoing: 'StatusSmall',
    }

    export const ColorsMap: Record<Request.Status, string> = {
      done: 'var(--green-900)',
      canceled: 'var(--amber-900)',
      failed: 'var(--red-900)',
      pending: 'var(--blue-900)',
      ongoing: 'var(--blue-900)',
    }

    export const BackgroundsMap: typeof ColorsMap = {
      done: 'var(--green-200)',
      canceled: 'var(--amber-200)',
      failed: 'var(--red-200)',
      pending: 'var(--blue-200)',
      ongoing: 'var(--blue-200)'
    }

    export const FinishedStatuses: string[] = [
      'canceled',
      'done',
      'error',
      'success',
      'failed',
    ]
  }

  /**
   * Renders the selected request's diagnostic details.
   * @param props Details props containing the selected request.
   * @returns Request details panel.
   */
  function RequestDetails({ request }: RequestDetails.Props) {
    const { t } = Locale.use();
    const sourceLinks = Request.Entity.sourceLinks(request);
    const errorMessages = Request.Entity.errorMessages(request);
    const counts = Request.Entity.recordCounts(request);
    const recordSummary = t('requests.recordSummary', {
      ingested: counts.records_ingested,
      skipped: counts.records_skipped,
      failed: counts.records_failed,
    });

    return (
      <Stack dir="column" ai="stretch" gap={12} className={s.details}>
        <Stack jc="space-between" className={s.detailsHeader}>
          <p>{t('requests.details')}</p>
          <Badge size="sm" mono variant="gray-subtle" value={request.req_type || request.type} />
        </Stack>
        <div className={s.detailGrid}>
          <DetailItem label={t('common.id')} value={request.id} />
          <DetailItem label={t('common.name')} value={request.name || t('common.none')} />
          <DetailItem
            label={t('common.status')}
            value={(
              <Stack gap={6} className={s.detailStatus}>
                <Status status={request.status} />
                <span>{request.status}</span>
              </Stack>
            )}
          />
          <DetailItem label={t('common.sources')} value={recordSummary} />
          <DetailItem
            label={t('common.context')}
            value={sourceLinks.length ? <LinkedValues values={sourceLinks.map((sourceLink) => `${sourceLink.contextName} - ${sourceLink.contextId}`)} /> : t('common.none')}
          />
          <DetailItem
            label={t('common.source')}
            value={sourceLinks.length ? <LinkedValues values={sourceLinks.map((sourceLink) => `${sourceLink.sourceName} - ${sourceLink.sourceId}`)} /> : t('common.none')}
          />
        </div>
        <Stack dir="column" ai="stretch" gap={6}>
          <p className={s.sectionTitle}>{t('requests.errors')}</p>
          <div className={s.errorList}>
            {errorMessages.length > 0 ? errorMessages.map((message, index) => (
              <p key={`${request.id}-error-${index}`} className={s.errorItem}>{message}</p>
            )) : (
              <p className={s.empty}>{t('requests.noErrorDetails')}</p>
            )}
          </div>
        </Stack>
      </Stack>
    )
  }

  /**
   * Renders one or more linked context/source labels without duplicating repeated values.
   * @param props Linked values to render.
   * @returns Linked values list.
   */
  function LinkedValues({ values }: LinkedValues.Props) {
    const uniqueValues = [...new Set(values)];

    return (
      <Stack dir="column" ai="stretch" gap={4} className={s.linkedValues}>
        {uniqueValues.map((value) => (
          <p key={value}>{value}</p>
        ))}
      </Stack>
    )
  }

  namespace LinkedValues {
    export interface Props {
      values: string[]
    }
  }

  namespace RequestDetails {
    export interface Props {
      request: Request.Type
    }
  }

  /**
   * Renders one label/value pair in the request details panel.
   * @param props Detail item props.
   * @returns Detail item element.
   */
  function DetailItem({ label, value }: DetailItem.Props) {
    return (
      <div className={s.detailItem}>
        <p className={s.detailLabel}>{label}</p>
        <div className={s.detailValue}>{value}</div>
      </div>
    )
  }

  namespace DetailItem {
    export interface Props {
      label: string
      value: ReactNode
    }
  }
}
