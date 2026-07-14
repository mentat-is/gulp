export class GulpIndexedDB {
    private DB_NAME: string;
    private DB_VERSION: number;
    private DB_STORE_NAMES: string[];
    // Ephemeral connections: No shared 'this.db' or 'activeConnections'
    private READMODE: IDBTransactionMode = "readonly";
    private WRITEMODE: IDBTransactionMode = "readwrite";

    /**
     * Initializes the GulpIndexedDB wrapper.
     * 
     * @param dbName The name of the database.
     * @param storeNames A single store name or an array of store names to be created/ensured.
     * @param version The version of the database. Incrementing this triggers onupgradeneeded.
     */
    constructor(dbName: string, storeNames: string | string[], version: number = 1) {
        this.DB_NAME = dbName;
        this.DB_STORE_NAMES = Array.isArray(storeNames) ? storeNames : [storeNames];
        this.DB_VERSION = version;
    }

    private OpenDB(): Promise<IDBDatabase> {
        return new Promise((resolve, reject) => {
            // First attempt to open with whatever version is current on disk
            const request = window.indexedDB.open(this.DB_NAME);

            request.onupgradeneeded = (ev) => {
                const db = (ev.target as IDBOpenDBRequest).result;
                this.DB_STORE_NAMES.forEach(storeName => {
                    if (!db.objectStoreNames.contains(storeName)) {
                        db.createObjectStore(storeName);
                    }
                });
            };

            request.onsuccess = (ev) => {
                const db = (ev.target as IDBOpenDBRequest).result;

                // LINEAR UPGRADE: Check if we are missing any stores we care about.
                // If so, we perform a single-step upgrade by re-opening with version + 1.
                // This avoids recursive loops.
                if (this.DB_STORE_NAMES.some(name => !db.objectStoreNames.contains(name))) {
                    const currentVersion = db.version;
                    db.close();

                    // Re-open with incremented version to trigger upgrade
                    const upgradeRequest = window.indexedDB.open(this.DB_NAME, currentVersion + 1);

                    upgradeRequest.onupgradeneeded = (uev) => {
                        const udb = (uev.target as IDBOpenDBRequest).result;
                        this.DB_STORE_NAMES.forEach(storeName => {
                            if (!udb.objectStoreNames.contains(storeName)) {
                                udb.createObjectStore(storeName);
                            }
                        });
                    };

                    upgradeRequest.onsuccess = (uev) => {
                        resolve((uev.target as IDBOpenDBRequest).result);
                    };

                    upgradeRequest.onerror = (uev) => {
                        console.warn("Error upgrading DB", uev);
                        reject(uev);
                    }

                } else {
                    resolve(db);
                }
            };

            request.onerror = (ev) => {
                console.warn("Error opening DB", ev);
                reject(ev);
            }
        });
    }

    private async MakeTransaction(storeName: string, mode: IDBTransactionMode): Promise<IDBTransaction> {
        const db = await this.OpenDB();

        // Handle concurrent version changes from other tabs to allow them to proceed
        db.onversionchange = () => {
            db.close();
        };

        if (!db.objectStoreNames.contains(storeName)) {
            db.close();
            // This should ideally strictly not happen due to OpenDB check, but guarding just in case
            throw new Error(`Store ${storeName} not found in DB ${this.DB_NAME}.`);
        }

        const tx = db.transaction(storeName, mode);

        // RESOURCE MANAGEMENT:
        // Ensure the connection is closed immediately after the transaction finishes.
        const close = () => {
            db.close();
        };

        tx.addEventListener('complete', close);
        tx.addEventListener('abort', close);
        tx.addEventListener('error', (err) => {
            console.warn("Transaction error", err);
            close();
        });

        return tx;
    }

    public GetConfiguration(configurationKey: string, storeName?: string): Promise<any> {
        const targetStore = storeName || this.DB_STORE_NAMES[0];
        return new Promise(async (resolve) => {
            const tx = await this.MakeTransaction(targetStore, this.READMODE);

            const store = tx.objectStore(targetStore);
            const request = store.get(configurationKey);
            request.onsuccess = () => {
                resolve(request.result);
            };
            request.onerror = (err) => {
                console.warn(
                    "error in request to get configuration",
                    configurationKey,
                    { err }
                );
            };
        });
    }

    public AddConfiguration(configuration: any, configurationKey: string, storeName?: string): Promise<Event> {
        const targetStore = storeName || this.DB_STORE_NAMES[0];
        return new Promise(async (resolve) => {
            const tx = await this.MakeTransaction(targetStore, this.WRITEMODE);
            tx.oncomplete = (ev) => {
                resolve(ev);
            };
            const store = tx.objectStore(targetStore);
            const request = store.add(configuration, configurationKey);
            request.onerror = (err) => {
                console.warn("error in request to add configuration", err);
            };
        });
    }

    public DeleteConfiguration(configurationKey: string, storeName?: string): Promise<Event> {
        const targetStore = storeName || this.DB_STORE_NAMES[0];
        return new Promise(async (resolve) => {
            const tx = await this.MakeTransaction(targetStore, this.WRITEMODE);
            tx.oncomplete = (ev) => {
                resolve(ev);
            };
            const store = tx.objectStore(targetStore);
            const request = store.delete(configurationKey);
            request.onerror = (err) => {
                console.warn(
                    "error in request to delete configuration",
                    configurationKey,
                    err
                );
            };
        });
    }

    public UpdateConfiguration(configuration: any, configurationKey: string, storeName?: string): Promise<Event> {
        const targetStore = storeName || this.DB_STORE_NAMES[0];
        return new Promise(async (resolve) => {
            const tx = await this.MakeTransaction(targetStore, this.WRITEMODE);
            tx.oncomplete = (ev) => {
                resolve(ev);
            };
            const store = tx.objectStore(targetStore);
            const request = store.put(configuration, configurationKey);
            request.onerror = (err) => {
                console.warn(
                    "error in request to update configuration",
                    configurationKey,
                    err
                );
            };
        });
    }

    public UpdateConfigurations(configurations: Array<[string, any]>, storeName?: string): Promise<Event | void> {
        if (!configurations.length) return Promise.resolve();

        const targetStore = storeName || this.DB_STORE_NAMES[0];
        return new Promise(async (resolve) => {
            const tx = await this.MakeTransaction(targetStore, this.WRITEMODE);
            tx.oncomplete = (ev) => {
                resolve(ev);
            };
            const store = tx.objectStore(targetStore);
            configurations.forEach(([configurationKey, configuration]) => {
                const request = store.put(configuration, configurationKey);
                request.onerror = (err) => {
                    console.warn(
                        "error in request to update configuration",
                        configurationKey,
                        err
                    );
                };
            });
        });
    }
}
