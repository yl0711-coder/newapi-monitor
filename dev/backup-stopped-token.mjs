// Stream-compressed backup of a stopped local Monitor. Never stops a service,
// copies a full uncompressed database, checkpoints SQLite, or removes data.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {spawn,execFileSync} from 'node:child_process';
import {Transform,Writable} from 'node:stream';
import {pipeline} from 'node:stream/promises';
import {stoppedBackupCodec} from './stopped-backup-codec.mjs';

const args=process.argv.slice(2),option=(key,fallback)=>args.includes(key)?args[args.indexOf(key)+1]:fallback;
const container=option('--container','newapi-monitor-prod-readonly-monitor-1'),imageID=option('--image-id','');
const out=path.resolve(option('--out','')),token=path.resolve(option('--token-dir','.local-test-kit/prod-readonly/token-data'));
assert.ok(args.includes('--out')&&imageID,'--out and --image-id are required');
const compressionFormat=option('--compression-format','gzip');
const compressionLevel=Number(option('--compression-level',compressionFormat==='brotli'?'4':'3'));
const codec=stoppedBackupCodec(compressionFormat,compressionLevel);
const maxBytes=8*1024**3,reserve=8*1024**3;
fs.mkdirSync(out,{recursive:true});
const inspect=()=>JSON.parse(execFileSync('docker',['inspect',container],{encoding:'utf8',windowsHide:true,timeout:15000}))[0];
const before=inspect();
assert.equal(before.State.Running,false,'Monitor must already be stopped');
assert.equal(before.Image,imageID,'stopped image differs from the approved rollback');
const dataMount=before.Mounts.find(v=>v.Destination==='/data');
assert.ok(dataMount?.Type==='volume'&&dataMount.Name,'named /data volume required');
const manifest={created_at:new Date().toISOString(),container,image_id:imageID,stopped_at:before.State.FinishedAt,
 format:codec.format,compression_level:compressionLevel,maximum_compressed_bytes:maxBytes,minimum_free_bytes:reserve,
 token_directory:token,data_volume:dataMount.Name,files:[],verified:false};
let total=0;
const capacity=()=>{const s=fs.statfsSync(out);assert.ok(s.bavail*s.bsize>=reserve,'backup would cross the disk reserve');};
capacity();
async function archive(name,input){
 const target=path.join(out,name+'.'+codec.extension),temporary=target+'.partial',plainHash=createHash('sha256'),compressedHash=createHash('sha256');
 let plain=0,compressed=0,lastCheck=0;
 const raw=new Transform({transform(chunk,encoding,done){plain+=chunk.length;plainHash.update(chunk);done(null,chunk)}});
 const compressedMeter=new Transform({transform(chunk,encoding,done){
  try{compressed+=chunk.length;total+=chunk.length;assert.ok(total<=maxBytes,'compressed backup exceeds its byte budget');
   if(total-lastCheck>64*1024**2){capacity();lastCheck=total}compressedHash.update(chunk);done(null,chunk);
  }catch(error){done(error)}
 }});
 console.log(JSON.stringify({stage:'compressing',name}));
 const progress=setInterval(()=>console.log(JSON.stringify({stage:'compressing',name,plain_bytes:plain,compressed_bytes:compressed})),30000);
 try{await pipeline(input,raw,codec.compress(),compressedMeter,fs.createWriteStream(temporary,{flags:'wx'}));}
 finally{clearInterval(progress)}
 const expected=plainHash.digest('hex'),stored=compressedHash.digest('hex');
 let decoded=0;const recovered=createHash('sha256');
 console.log(JSON.stringify({stage:'verifying_decompression',name,plain_bytes:plain,compressed_bytes:compressed}));
 await pipeline(fs.createReadStream(temporary),codec.decompress(),new Writable({write(chunk,encoding,done){decoded+=chunk.length;recovered.update(chunk);done()}}));
 assert.equal(decoded,plain,'decompressed length mismatch');assert.equal(recovered.digest('hex'),expected,'decompressed hash mismatch');
 fs.renameSync(temporary,target);
 const entry={name:path.basename(target),plain_bytes:plain,compressed_bytes:compressed,plain_sha256:expected,compressed_sha256:stored,decompression_verified:true};
 manifest.files.push(entry);return entry;
}
for(const name of ['token.db','token.db-wal','token.db-shm']){
 const source=path.join(token,name);if(!fs.existsSync(source))continue;
 const prior=fs.statSync(source);assert.ok(prior.isFile()&&!fs.lstatSync(source).isSymbolicLink());
 const record=await archive(name,fs.createReadStream(source));
 const after=fs.statSync(source);assert.equal(after.size,prior.size);assert.equal(after.mtimeMs,prior.mtimeMs);assert.equal(record.plain_bytes,prior.size);
}
assert.ok(manifest.files.some(v=>v.name==='token.db.'+codec.extension),'Token database is missing');
// Preserve runtime data/configuration, excluding older backup directories.
const child=spawn('docker',['run','--rm','--pull=never','--network=none','--read-only','--user','1000:1000','--cap-drop=ALL',
 '--mount',`type=volume,source=${dataMount.Name},target=/data,readonly`,'--entrypoint','tar','alpine:3.23',
 '-C','/data','--exclude=./backups','--exclude=./nginx-evidence-migration-backups','-cf','-','.'],{windowsHide:true,stdio:['ignore','pipe','pipe']});
let stderr='';child.stderr.on('data',chunk=>{stderr=(stderr+chunk).slice(-4096)});
const exited=new Promise(resolve=>child.on('close',code=>resolve(code)));
try{await archive('monitor-data.tar',child.stdout);assert.equal(await exited,0,'data archive failed: '+stderr)}
catch(error){child.kill();throw error}
const after=inspect();assert.equal(after.State.Running,false);assert.equal(after.State.StartedAt,before.State.StartedAt);assert.equal(after.State.FinishedAt,before.State.FinishedAt);
capacity();manifest.verified=true;manifest.compressed_bytes=total;manifest.completed_at=new Date().toISOString();
fs.writeFileSync(path.join(out,'manifest.json'),JSON.stringify(manifest,null,2)+'\n',{flag:'wx'});
console.log(JSON.stringify({stage:'verified',compressed_bytes:total,files:manifest.files.length,manifest:path.join(out,'manifest.json')}));
