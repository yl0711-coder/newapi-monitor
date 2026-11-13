import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes,createHash} from 'node:crypto';
import {Readable,Writable} from 'node:stream';
import {pipeline} from 'node:stream/promises';
import {stoppedBackupCodec} from '../stopped-backup-codec.mjs';

const hash=b=>createHash('sha256').update(b).digest('hex');
async function transform(bytes,codec){
 const chunks=[];const source=async function*(){for(let i=0;i<bytes.length;i+=49157)yield bytes.subarray(i,i+49157)};
 await pipeline(Readable.from(source()),codec,new Writable({write(chunk,encoding,done){chunks.push(chunk);done()}}));
 return Buffer.concat(chunks);
}
for(const format of ['gzip','brotli'])test(format+' streaming backup preserves length and SHA256, including random SQLite-like bytes',async()=>{
 const codec=stoppedBackupCodec(format),input=Buffer.concat([Buffer.from('SQLite format 3\0'),randomBytes(1024*1024),Buffer.from('{"source_epoch":"fixture","request_id":"fixture"}\n'.repeat(8192))]);
 const encoded=await transform(input,codec.compress()),decoded=await transform(encoded,codec.decompress());
 assert.equal(decoded.length,input.length);assert.equal(hash(decoded),hash(input));
 const broken=encoded.subarray(0,Math.floor(encoded.length/2));
 await assert.rejects(transform(broken,codec.decompress()));
});
test('backup codec rejects unsupported formats and out-of-range levels; gzip remains the default',()=>{
 assert.equal(stoppedBackupCodec().format,'gzip-stream-v1');assert.equal(stoppedBackupCodec().level,3);
 assert.equal(stoppedBackupCodec('brotli').extension,'br');assert.equal(stoppedBackupCodec('brotli').level,4);
 assert.throws(()=>stoppedBackupCodec('zip'));for(const level of [-1,0,10,NaN,1.5])assert.throws(()=>stoppedBackupCodec('brotli',level));
});
