import { BaseAdapter, IBlockRef } from 'ah-memory-fs';
import OSS from 'ali-oss';
import Path from 'path-browserify';

export class AliOssAdapter extends BaseAdapter {
  static async create(store: OSS, prefixPath: string) {
    const nd = new AliOssAdapter(store, prefixPath);
    await nd.setup();
    return nd;
  }

  constructor(private readonly _store: OSS, private readonly prefixPath: string) {
    super();
    if (prefixPath.startsWith('/')) throw new Error('prefixPath cannot start with `/`');
  }

  async setup(): Promise<void> {}
  async dispose(): Promise<void> {}

  async read(id: string): Promise<ArrayBuffer> {
    const path = Path.join(this.prefixPath, id);

    const rsp = await this._store.get(path);
    if (!rsp.content) throw new Error(`get ${id} response empty`);

    return (rsp.content as Uint8Array).buffer;
  }

  async write(id: string, data: ArrayBuffer): Promise<void> {
    const path = Path.join(this.prefixPath, id);
    await this._store.multipartUpload(path, new Blob([data]), {});
  }

  async del(id: string): Promise<void> {
    const path = Path.join(this.prefixPath, id);
    await this._store.delete(path);
  }

  async getBlockRefs(): Promise<IBlockRef[]> {
    const prefix = Path.join(this.prefixPath, '/'); // 确保 prefix 末尾有一个 `/`
    const blocks: IBlockRef[] = [];

    const maxKeys = 200;
    let nextContinuationToken: string | undefined;

    while (1) {
      // https://github.com/ali-sdk/ali-oss#listv2query-options
      const rsp = await (this._store as any).listV2({
        'max-keys': maxKeys,
        prefix,
        'continuation-token': nextContinuationToken,
      });

      nextContinuationToken = rsp.nextContinuationToken;
      rsp.objects.forEach((t: any) => {
        blocks.push({ key: Path.relative(prefix, t.name), size: t.size });
      });

      if (rsp.keyCount < maxKeys) break; // 已经取完了，可以退出
    }

    return blocks;
  }
}
