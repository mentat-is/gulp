import { renderToStaticMarkup } from 'react-dom/server'
import { Icon } from '@/ui/Icon'
import { Glyph } from '@/entities/Glyph'

const PLACEHOLDER_CANVAS = document.createElement('canvas');
PLACEHOLDER_CANVAS.width = 1;
PLACEHOLDER_CANVAS.height = 1;
const PLACEHOLDER_IMAGE = new Image(1, 1);

export function getCanvasIcon({ name, ...props }: CanvasIcon.Props): HTMLImageElement {
  const key = JSON.stringify({ name, ...props });
  const icon = CanvasIcon.cache.get(key);
  if (icon) {
    return icon;
  }

  const glyphSrc = Glyph.Images.get(name);
  if (glyphSrc) {
    createImage(glyphSrc).then(image => {
      CanvasIcon.cache.set(key, image);
      CanvasIcon.onIconLoad?.();
    }).catch(() => undefined);
    return PLACEHOLDER_IMAGE;
  }

  // Start async load but return placeholder instead of throwing
  const svg = renderToStaticMarkup(<Icon name={name} {...props} />);
  createImageFromSVG(svg).then(image => {
    CanvasIcon.cache.set(key, image);
    CanvasIcon.onIconLoad?.();
  }).catch(() => undefined);

  return PLACEHOLDER_IMAGE;
}

/**
 * Creates a transferable bitmap for a built-in or custom Canvas glyph.
 * @param props Icon name and visual properties used to render the bitmap.
 * @returns Newly allocated bitmap whose ownership may be transferred to a worker.
 */
export async function createCanvasIconBitmap({
  name,
  ...props
}: CanvasIcon.Props): Promise<ImageBitmap> {
  const glyphSource = Glyph.Images.get(name)
  const size = typeof props.size === 'number' ? props.size : 16
  const image = glyphSource
    ? await createImageFromBlob(await fetch(glyphSource).then((response) => response.blob()))
    : await createImageFromSVG(renderToStaticMarkup(<Icon name={name} {...props} />))

  return createBitmapFromImage(image, size)
}

/**
 * Creates the transparent placeholder used when a worker icon cannot be decoded.
 * @returns One-pixel bitmap that settles the worker asset request without a retry loop.
 */
export function createCanvasIconPlaceholderBitmap(): Promise<ImageBitmap> {
  return createImageBitmap(new ImageData(1, 1))
}

const createImage = (src: string): Promise<HTMLImageElement> => {
  return new Promise((resolve, reject) => {
    const img = new Image()
    img.onload = () => resolve(img)
    img.onerror = reject
    img.src = src
  })
}

const createImageFromSVG = (svgString: string): Promise<HTMLImageElement> => {
  return new Promise((resolve, reject) => {
    const img = new Image()
    const svgBlob = new Blob([svgString], { type: 'image/svg+xml' })
    const url = URL.createObjectURL(svgBlob)

    img.onload = () => {
      URL.revokeObjectURL(url)
      resolve(img)
    }
    img.onerror = reject
    img.src = url
  })
}

/**
 * Decodes a binary image blob through an HTML image element.
 * @param blob Image data fetched from a registered custom glyph source.
 * @returns Loaded image element ready for canvas rasterization.
 */
const createImageFromBlob = (blob: Blob): Promise<HTMLImageElement> => {
  return new Promise((resolve, reject) => {
    const img = new Image()
    const url = URL.createObjectURL(blob)

    img.onload = () => {
      URL.revokeObjectURL(url)
      resolve(img)
    }
    img.onerror = (event) => {
      URL.revokeObjectURL(url)
      reject(event)
    }
    img.src = url
  })
}

/**
 * Converts a decoded image into a pixel-backed bitmap suitable for worker transfer.
 * @param image Decoded SVG or raster image.
 * @param size Square bitmap dimension in physical pixels.
 * @returns ImageBitmap containing rasterized pixels.
 */
const createBitmapFromImage = (
  image: CanvasImageSource,
  size: number,
): Promise<ImageBitmap> => {
  const canvas = document.createElement('canvas')
  canvas.width = size
  canvas.height = size
  const context = canvas.getContext('2d')
  if (!context) {
    throw new Error('Unable to create canvas context for icon bitmap')
  }
  context.clearRect(0, 0, size, size)
  context.drawImage(image, 0, 0, size, size)
  return createImageBitmap(canvas)
}

export const CanvasIcon = {
  cache: new Map<string, HTMLImageElement>(),
  onIconLoad: null as (() => void) | null,
};

export namespace CanvasIcon {
  export interface Props extends Icon.Props {

  }
}
