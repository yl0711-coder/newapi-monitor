import assert from 'node:assert/strict';
import {createGzip,createGunzip,createBrotliCompress,createBrotliDecompress,constants} from 'node:zlib';

// Both formats are standard, streaming archives. Selection never changes the
// archive byte budget, disk reserve or full decompression/hash verification.
export function stoppedBackupCodec(format='gzip',level=format==='brotli'?4:3){
 assert.ok(['gzip','brotli'].includes(format),'compression format must be gzip or brotli');
 assert.ok(Number.isInteger(level)&&level>=1&&level<=9,'compression level must be 1 through 9');
 return format==='gzip'
  ? {format:'gzip-stream-v1',extension:'gz',level,compress:()=>createGzip({level}),decompress:()=>createGunzip()}
  : {format:'brotli-stream-v1',extension:'br',level,compress:()=>createBrotliCompress({params:{[constants.BROTLI_PARAM_QUALITY]:level}}),decompress:()=>createBrotliDecompress()};
}
