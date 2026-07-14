// copies ../gulp-paid-plugins/src/gulp-paid-plugins/plugins/ui -> ./src/plugins (merge, do not remove existing files)

const fs = require('fs');
const path = require('path');

const src_path = path.resolve(__dirname, '..', '..', 'gulp-paid-plugins', 'src', 'gulp-paid-plugins', 'plugins', 'ui');
const dest_root = path.resolve(__dirname, '..', 'src', 'plugins');

// check existence of a path
async function path_exists(p) {
  try {
    await fs.promises.access(p);
    return true;
  } catch {
    return false;
  }
}

// copy a single file (overwrites)
async function copy_file(src, dest) {
  await fs.promises.mkdir(path.dirname(dest), { recursive: true });
  await fs.promises.copyFile(src, dest);
}

// recursively copy directory contents from src -> dest (merge)
async function copy_recursive(src, dest) {
  const entries = await fs.promises.readdir(src, { withFileTypes: true });
  for (const ent of entries) {
    const src_ent = path.join(src, ent.name);
    const dest_ent = path.join(dest, ent.name);

    if (ent.isSymbolicLink()) {
      // resolve symlink target then copy whatever it points to
      const real = await fs.promises.realpath(src_ent);
      const stat = await fs.promises.stat(real);
      if (stat.isDirectory()) {
        await copy_recursive(real, dest_ent);
      } else {
        await copy_file(real, dest_ent);
      }
    } else if (ent.isDirectory()) {
      await copy_recursive(src_ent, dest_ent);
    } else if (ent.isFile()) {
      await copy_file(src_ent, dest_ent);
    }
  }
}

(async () => {
  try {
    if (!(await path_exists(src_path))) {
      console.log('no paid-ui plugins found at', src_path);
      process.exit(0);
    }

    // ensure destination root exists
    await fs.promises.mkdir(dest_root, { recursive: true });

    // copy contents from src_path into dest_root (merge)
    await copy_recursive(src_path, dest_root);

    console.log('copied paid-ui plugins into', dest_root);
    process.exit(0);
  } catch (err) {
    console.error('copy_paid_plugins error:', err);
    process.exit(1);
  }
})();
