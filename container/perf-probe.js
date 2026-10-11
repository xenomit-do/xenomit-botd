const fs = require('fs');
const net = require('net');
const sleep = ms => new Promise(r=>setTimeout(r,ms));
async function query(command) {
  return new Promise((resolve,reject)=>{
    const file=fs.readdirSync('/tmp').find(n=>/^xenomit_flash_\d+\.sock$/.test(n));
    const s=net.connect('/tmp/'+file);let data='';s.setTimeout(12000,()=>{s.destroy();reject(Error('timeout'))});s.on('error',reject);
    s.on('connect',()=>s.write(command+'\n'));
    s.on('data',c=>{data+=c;if(data.includes('\n')){s.end('ACK\n');resolve(data.trim())}});
  });
}
function percentile(a,p){const b=[...a].sort((a,b)=>a-b);return b[Math.min(b.length-1,Math.floor(b.length*p))]}
(async()=>{
  let fd;let header=Buffer.alloc(128);const seq=Buffer.alloc(4);
  const deadline=Date.now()+120000;
  while(Date.now()<deadline){
    const file=fs.readdirSync('/dev/shm').find(n=>/^xenomit_state_\d+$/.test(n));
    if(file){if(fd!==undefined)fs.closeSync(fd);fd=fs.openSync('/dev/shm/'+file,'r');fs.readSync(fd,header,0,128,0);if(header.readUInt32LE(0)===0x4d545358 && Date.now()-Number(header.readBigUInt64LE(24))<500)break;}
    await sleep(250);
  }
  if(fd===undefined || header.readUInt32LE(0)!==0x4d545358)throw Error('native frame unavailable');
  await sleep(Number(process.env.XENOMIT_PERF_WARMUP_MS || 10000)); // exclude bootstrap/JIT
  const costs=[],ages=[],counts=[],reads=[],fps=[];let nextMetrics=0;let previous=-1n,firstFrame,lastFrame,firstAt,lastAt;
  const start=Date.now();
  while(Date.now()-start<Number(process.env.XENOMIT_PERF_SAMPLE_MS || 20000)){
    const t=process.hrtime.bigint();fs.readSync(fd,header,0,128,0);reads.push(Number(process.hrtime.bigint()-t)/1000);
    fs.readSync(fd,seq,0,4,8);
    if((header.readUInt32LE(8)&1) || seq.readUInt32LE(0)!==header.readUInt32LE(8))continue;
    if(Date.now()-Number(header.readBigUInt64LE(24))>1500)throw Error("native frame became stale");
    const f=header.readBigUInt64LE(16);
    if(f!==previous){previous=f;costs.push(header.readUInt32LE(32));ages.push(Date.now()-Number(header.readBigUInt64LE(24)));counts.push(header.readUInt32LE(80));lastFrame=f;lastAt=Date.now();if(!firstAt){firstFrame=f;firstAt=lastAt;}}
    if(Date.now()>=nextMetrics){
      nextMetrics=Date.now()+1000;
      const reply=await query('METRICS');
      if(reply.startsWith('OK ')){const m=JSON.parse(reply.slice(3));if(m && Number.isFinite(m.fps))fps.push(m.fps);}
    }
    await sleep(2);
  }
  let n;
  for(let attempt=0;attempt<20;attempt++){
    fs.readSync(fd,header,0,128,0);if(header.readUInt32LE(8)&1)continue;
    const len=header.readUInt32LE(100),off=header.readUInt32LE(96);
    if(len>1024*1024 || 589952+off+len>2097152)throw Error('invalid navigation bounds');
    const nav=Buffer.alloc(len);fs.readSync(fd,nav,0,len,589952+off);
    fs.readSync(fd,seq,0,4,8);
    if(seq.readUInt32LE(0)!==header.readUInt32LE(8))continue;
    n=JSON.parse(nav.toString());break;
  }
  if(!n || !n.geometry_ready)throw Error('fresh obstacle geometry is unavailable');
  console.log(JSON.stringify({samples:costs.length,captureUs:{median:percentile(costs,.5),p95:percentile(costs,.95),max:Math.max(...costs)},headerReadUs:{median:percentile(reads,.5),p95:percentile(reads,.95)},flashFps:{median:percentile(fps,.5),min:Math.min(...fps),max:Math.max(...fps)},captureHz:Number(lastFrame-firstFrame)*1000/(lastAt-firstAt),ageMs:{median:percentile(ages,.5),p95:percentile(ages,.95)},entityRange:[Math.min(...counts),Math.max(...counts)],navigation:{mapId:n.map_id,geometryReady:n.geometry_ready,obstacles:n.obstacles.length,portals:n.portals.length},metrics:await query('METRICS'),bridge:await query('BRIDGE'),frameCall:JSON.parse((await query('FINVOKE\txenomit\tclientFeatures\t[]')).slice(3)).status}));
  fs.closeSync(fd);
})().catch(e=>{console.error(e.message);process.exitCode=1});
