export namespace Images {
  export function LeftCap() {
    return (
      <svg
        xmlns='http://www.w3.org/2000/svg'
        viewBox='0 0 60 42'
        width='60'
        height='42'
        preserveAspectRatio='none'
        style={{
          position: 'absolute',
          top: -1,
          left: 88,
          zIndex: 1,
          pointerEvents: 'none',
          shapeRendering: 'optimizeSpeed'
        }}
      >
        <defs>
          <clipPath id='clipCurve'>
            <path
              d='M1.5 0.5 L10.5 0.5 C18.5 0.5 23.5 4 26 11 L35 30 C38 36 45 41 53 41 H60 V0 H0 Z'
            />
          </clipPath>
        </defs>
        <rect
          width='60'
          height='42'
          fill='var(--background-200)'
          clipPath='url(#clipCurve)'
        />
        <path
          d='M1.5 0.5 L10.5 0.5 C18.5 0.5 23.5 4 26 11 L35 30 C38 36 45 41 53 41 H60'
          fill='none'
          stroke='var(--gray-400)'
          strokeWidth='1'
        />
        <rect
          x={-2}
          y={0}
          width={10}
          height={1}
          fill='var(--gray-400)'
        />
      </svg>
    );
  }

  export function MiddleLine() {
    return (
      <svg
        xmlns='http://www.w3.org/2000/svg'
        viewBox='0 0 100 42'
        preserveAspectRatio='none'
        style={{
          position: 'absolute',
          zIndex: 1,
          top: -1,
          left: 'calc(143px)',
          width: 'calc(100% - 331px)',
          height: 42,
          shapeRendering: 'optimizeSpeed'
        }}
      >
        <rect width='100%' height='100%' fill='var(--background-200)' />
        <rect width='100%' height='100%' fill='var(--background-200)' />
        <line
          x1='0'
          y1='41'
          x2='100'
          y2='41'
          stroke='var(--gray-400)'
          strokeWidth='1'
        />
      </svg>
    )
  }

  export function RightCap() {
    return (
      <svg
        xmlns='http://www.w3.org/2000/svg'
        viewBox='0 0 60 42'
        width='60'
        height='42'
        preserveAspectRatio='none'
        style={{
          position: 'absolute',
          top: -1,
          right: 128,
          transform: 'scaleX(-1)',
          zIndex: 1,
          pointerEvents: 'none',
          shapeRendering: 'optimizeSpeed'
        }}
      >
        <defs>
          <clipPath id='clipCurveRight'>
            <path d='M1.5 0.5 L10.5 0.5 C18.5 0.5 23.5 4 26 11 L35 30 C38 36 45 41 53 41 H60 V0 H0 Z' />
          </clipPath>
        </defs>
        <rect width='60' height='42' fill='var(--background-200)' clipPath='url(#clipCurveRight)' />
        <path
          d='M1.5 0.5 L10.5 0.5 C18.5 0.5 23.5 4 26 11 L35 30 C38 36 45 41 53 41 H60'
          fill='none'
          stroke='var(--gray-400)'
          strokeWidth='1'
        />
        <rect x={-2} y={0} width={10} height={1} fill='var(--gray-400)' />
      </svg>
    );
  }
}
