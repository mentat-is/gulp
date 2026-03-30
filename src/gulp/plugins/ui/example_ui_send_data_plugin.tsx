import React, { forwardRef, useImperativeHandle, useRef } from 'react';
import { Doc } from '@/entities/Doc';
import { Stack } from '@/ui/Stack';

/**
 * Public interface exposed by the plugin to the parent component (SendData.banner.tsx).
 *
 * The parent banner calls `onDone()` when the user clicks the "Done" button.
 * This allows the plugin to execute its data sending logic without the
 * banner needing to know the plugin's internal details.
 */
export interface SendDataPluginRef {
  /** Called by the banner when the user clicks "Done" */
  onDone: () => void;
}

/**
 * Props received by the plugin via `Extension.Component`.
 * The parent banner passes these props to the plugin when it is dynamically mounted.
 */
export interface SendDataPluginProps {
  /** The selected event, passed from the parent banner */
  event: Doc.Type;
}

/**
 * Example plugin component for the "send_data" type.
 *
 * Communication flow:
 * 1. `SendData.banner.tsx` mounts this component via `<Extension.Component>`,
 *    passing a `ref` and `props` (e.g., the current event).
 * 2. Using `useImperativeHandle`, this component exposes `onDone` on the ref.
 * 3. When the user clicks the "Done" button in the banner, the banner calls
 *    `pluginRef.current?.onDone()`.
 * 4. `onDone` reads the current value of the input and shows an alert with that text.
 */
const ExampleSendData = forwardRef<SendDataPluginRef, SendDataPluginProps>(
  function ExampleSendData({ event }, ref) {
    /** Ref to read the input field value without needing to use React state */
    const inputRef = useRef<HTMLInputElement>(null);

    /**
     * Exposes the public interface to the parent component.
     * The parent banner accesses `onDone` via the ref passed to this component.
     */
    useImperativeHandle(ref, () => ({
      onDone() {
        const value = inputRef.current?.value ?? '';
        // Show an alert with the text entered by the user
        alert(`Sending data:\n${value}`);
      },
    }));

    return (
      <Stack dir="column" ai="flex-start" gap={12} style={{ width: '100%' }}>
        {/* Plugin title */}
        <h6 style={{ margin: 0 }}>Example Send Data Plugin</h6>

        {/*
         * Input field: its value is read via `inputRef` when the banner calls
         * `onDone()` — there's no need to manage React state for this.
         */}
        <input
          ref={inputRef}
          type="text"
          placeholder="Write something to send..."
          defaultValue=""
          style={{
            width: '100%',
            padding: '8px 12px',
            borderRadius: '6px',
            border: '1px solid var(--outline)',
            background: 'var(--background-2)',
            color: 'var(--text)',
            fontSize: 14,
            boxSizing: 'border-box',
          }}
        />
      </Stack>
    );
  }
);

/**
 * Default export required by the plugin system (Extension.context.tsx).
 * The context dynamically loads this file and uses `component.default` as the
 * React component to mount.
 */
export default ExampleSendData;
