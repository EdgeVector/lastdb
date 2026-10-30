// Actual app operations on an owned CI node. Raw measurements stay outside Git.
import { parseArgs } from "node:util";
import { join, resolve } from "node:path";
import { readFileSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
const {values:a}=parseArgs({options:Object.fromEntries(
  ["app","baseline","candidate","socket","config","output","mode","label"].map((name)=>[name,{type:"string"}]))});
for(const key of ["app","baseline","candidate","socket","config","output","mode","label"]) if(!a[key]) throw Error("Missing "+key);
const load=(root,name)=>import(join(resolve(root),"src",name+".ts"));
const require=(ok,message)=>{if(!ok)throw Error(message)};
const candidate=a.candidate, old=a.baseline, active=a.label==="baseline"?old:candidate;
const quiet=()=>{};
const samples={};
const measure=async(name,operation)=>{const start=performance.now();const result=await operation();
  (samples[name]??=[]).push(performance.now()-start);return result;};
const body=(n)=>createHash("sha256").update(String(n)).digest("hex").repeat(32);
const time=(n)=>new Date(1700000000000+n*1000).toISOString();
const slug=(n)=>"bench-"+String(n).padStart(6,"0");
const count=256, iterations=128;
async function client(root,cfg,canonicalVocabulary=false) {
  const {newNodeClient}=await load(root,"client");
  return newNodeClient({baseUrl:"http://127.0.0.1",userHash:cfg?.userHash??"",
    socketPath:a.socket,schemaFieldAliases:cfg?.schemaFieldAliases,canonicalVocabulary,timeoutMs:120000});
}
async function setup() {
  let node=await client(old);
  let identity=await node.autoIdentity();
  const userHash=identity.provisioned?identity.userHash:(await node.bootstrap("CI app latency")).userHash;
  node=await client(old,{userHash},true);
  const schemas=await load(old,"schemas"), vocabulary=await load(old,"canonical-vocabulary");
  const entries=a.app==="fkanban"?[...schemas.UNIQUE_SCHEMAS,...schemas.EXTRA_SCHEMAS]:schemas.UNIQUE_SCHEMAS;
  const schemaHashes={},schemaFieldAliases={};
  // Catalog setup uses the real Mini declaration route and registered app definitions.
  for(let start=0;start<entries.length;start+=4) {
    const resolved=await Promise.all(entries.slice(start,start+4).map(async(entry)=>({
      entry,receipt:await node.declareAppSchema(schemas.OWNER_APP_ID,entry.schema.schema)})));
    for(const {entry,receipt} of resolved) {
      require(receipt.canonical,"App declaration lacks its canonical address");
      const keys=a.app==="fkanban"?[entry.key]:schemas.schemaConfigKeys(entry);
      for(const key of keys)schemaHashes[key]=receipt.canonical;
      Object.assign(schemaFieldAliases,vocabulary.profilesFor(entry.schema.schema,[receipt.canonical,receipt.schema]));
    }
  }
  const cfg={configVersion:1,nodeUrl:"http://127.0.0.1",schemaServiceUrl:"registered-definition-service",
    userHash,nodeSocketPath:a.socket,schemaHashes,schemaFieldAliases,appIdentityEnforce:false,
    designSchemaHash:schemaHashes.design,taskSchemaHash:schemaHashes.task,enforceLivePrMilestone:false};
  const config=await load(candidate,"config");config.writeConfig(cfg,a.config);
  node=await client(old,cfg);
  if(a.app==="fkanban") await kanbanSeed(node,cfg);
  else await brainSeed(node,cfg);
  if(a.label==="candidate") {
    const {convertMultiKey}=await load(candidate,"commands/multi-key-convert");
    const report=await convertMultiKey(await client(candidate,cfg),cfg,{apply:true,maintenance:true,configPath:a.config});
    require(report.phase==="complete"&&report.selected_keys===report.expected_selected_keys,"App conversion is incomplete");
    writeFileSync(a.output+"/conversion.json",JSON.stringify(report,null,2)+"\n");
  }
  console.log(JSON.stringify({phase:"seeded",app:a.app,label:a.label,logical_records:count,payload_bytes:count*body(0).length}));
}
async function kanbanSeed(node,cfg) {
  const r=await load(old,"record"), lists=await load(old,"card-list-index");
  const board={slug:"bench",title:"Benchmark",body:"Generated CI board",columns:["todo","doing","done"],created_at:time(0),updated_at:time(0)};
  await node.createRecord({schemaHash:cfg.schemaHashes.board,keyHash:board.slug,fields:r.boardToFields(board)});
  await lists.writeBoardListIndex(node,cfg,[r.toBoardSummary(board)]);
  for(let n=0;n<2;n++) await r.upsertMilestoneRecord(node,cfg,{slug:"milestone-"+n,title:"Milestone "+n,
    body:"Generated CI milestone",board:"bench",state:"active",position:String(n),north_star:"",driver:"",
    deps:[],proof_card:"",proof_status:"not_required",block_reason:"",created_at:time(0),updated_at:time(0),completed_at:""},false);
  // Repeated single-record app creates are the workload; no production bulk loop is introduced.
  for(let n=0;n<count;n++) {
    const card={...r.emptyStructuredFields(),slug:slug(n),title:"Record "+n,body:body(n),board:"bench",
      column:n%2?"todo":"doing",position:String(n*10),assignee:"",created_by:"ci",tags:[],deps:[],surfaces:[],
      created_at:time(n),updated_at:time(n),done_at:"",first_doing_at:"",kind:"meta",milestone:"milestone-"+n%2};
    await r.createCardRecord({node,cfg},card);
  }
}
async function brainWrite(root,node,cfg,type,previous,next) {
  const r=await load(root,"record"),p=await load(root,"resident-write-plan");
  const plan=await p.buildResidentWritePlan({node,cfg,type,schemaHash:cfg.schemaHashes[type],previous,next,
    primaryFields:r.buildRecordFields(type,next),now:next.updated_at});
  const result=await p.commitResidentWritePlan({node,plan,type,slug:next.slug,durable:true,failOnIndexPartial:true});
  require(result.durability==="durable","The app mutation lacks a durable receipt");
}
async function brainSeed(node,cfg) {
  const {RECORDS}=await load(old,"schemas");
  for(let n=0;n<count;n++) {
    const type=n%2?"task":"design";
    const record={slug:slug(n),title:"Record "+n,body:body(n),status:RECORDS[type].defaultStatus,tags:["benchmark"],
      created_at:time(n),updated_at:time(n),...(type==="task"?{design_slug:slug(0)}:{})};
    await brainWrite(old,node,cfg,type,null,record);
  }
}
async function kanbanMeasure(node,cfg) {
  const r=await load(active,"record"),m=await load(active,"milestone-cards");
  const expected=new Map();
  for(let n=0;n<count;n++) expected.set(slug(n),{column:n%2?"todo":"doing",position:String(n*10)});
  for(let n=0;n<iterations;n++) {
    const id=slug(n%count);
    const point=await measure("point_read",()=>r.findCard(node,cfg,id));
    require(point?.body===body(n%count),"Card body differs");
    const next={...point,column:n%2?"doing":"todo",position:String(10000+n),updated_at:time(count+n)};
    await measure("update",()=>r.updateCardRecord({node,cfg},next,undefined,point));
    expected.set(id,{column:next.column,position:next.position});
    const board=await measure("board_summary",()=>r.listCardsOnBoard(node,cfg,"bench"));
    require(board.length===count&&new Set(board.map((card)=>card.slug)).size===count,"Board membership differs");
    for(const card of board)require(JSON.stringify({column:card.column,position:card.position})===JSON.stringify(expected.get(card.slug)),"Board fields are stale");
    const ordered=board.map((card)=>card.column+"#"+card.position.padStart(8,"0")+"#"+card.slug);
    require(JSON.stringify(ordered)===JSON.stringify([...ordered].sort()),"Board order differs");
    const members=await measure("milestone_summary",()=>m.listMilestoneCardsPartition(node,cfg,"milestone-1"));
    require(members.length===count/2&&members.every((card)=>Number(card.slug.slice(6))%2===1),"Milestone membership differs");
  }
  if(a.label==="candidate") {
    const g=await load(candidate,"guarded-factory"),s=await load(candidate,"guarded-snapshot");
    for(let n=0;n<16;n++) {
      const witness=await s.captureSnapshot(node,cfg,slug(0));
      const intended={...witness.fields,title:"guarded-"+n};
      const start=performance.now();
      const attempts=await Promise.allSettled([g.compoundUpdate({node,cfg,slug:slug(0)},witness,intended,["title"]),
        g.compoundUpdate({node,cfg,slug:slug(0)},witness,{...intended,title:"rival-"+n},["title"])]);
      require(attempts.filter((result)=>result.status==="fulfilled").length===1,"Competing guards did not select exactly one writer");
      (samples.native_guarded_competition??=[]).push(performance.now()-start);
    }
  }
  return {logical_records:count,body_bytes:count*body(0).length,guarded_competitions:a.label==="candidate"?16:0};
}
async function brainMeasure(node,cfg) {
  const r=await load(active,"record"),l=await load(active,"commands/list"),types=await load(active,"schemas");
  for(let n=0;n<iterations;n++) {
    const type=n%2?"task":"design",id=slug(n%count);
    const point=await measure("point_read",()=>r.findBySlug(node,type,cfg.schemaHashes[type],id,cfg));
    require(point?.body===body(n%count),"Brain body differs");
    const next={...point,title:"Updated "+n,updated_at:time(count+n)};
    await measure("update",()=>brainWrite(active,node,cfg,type,point,next));
    const summaries=await measure("type_summary",()=>r.listRecords(node,type,cfg,{summary:true}));
    require(summaries.length===count/2&&new Set(summaries.map((item)=>item.slug)).size===count/2,"Type membership differs");
    require(summaries.some((item)=>item.slug===id&&item.title===next.title),"Type summary is stale");
    let page;
    await measure("recent_page",()=>l.listCmd({cfg,limit:20,json:true,print:quiet,printErr:quiet,onResult:(value)=>{page=value}}));
    require(page?.items.length===20&&page.items[0].slug===id,"Recent page differs");
  }
  let total;
  await l.listCmd({cfg,count:true,json:true,print:quiet,printErr:quiet,onResult:(value)=>{total=value.count}});
  require(total===count,"Brain logical count differs");
  if(a.label==="candidate") {
    const named=await load(candidate,"named-records");
    const all=[];let cursor;
    for(let page=0;page<100;page++) {
      const result=await named.namedRecordPage(node,cfg,{},17,cursor);
      all.push(...result.items);
      if(!result.has_more)break;
      require(result.next_page&&result.next_page!==cursor,"The app cursor stalled");cursor=result.next_page;
    }
    require(all.length===count&&new Set(all.map((item)=>item.type+"/"+item.slug)).size===count,"App cursor membership differs");
    const children=await named.namedRecordPage(node,cfg,{type:"task",design:slug(0)},500);
    require(children.items.length===count/2,"Task design membership differs");
    // The same canonical field may hold different values in separate record kinds.
    const next={slug:"shared-slug",title:"Design only",body:body(500),status:types.RECORDS.design.defaultStatus,tags:[],created_at:time(500),updated_at:time(500)};
    await brainWrite(candidate,node,cfg,"design",null,next);
    await brainWrite(candidate,node,cfg,"task",null,{...next,title:"Task only",body:body(501),status:types.RECORDS.task.defaultStatus,design_slug:slug(0)});
    const rows=await Promise.all(["design","task"].map((type)=>r.findBySlug(node,type,cfg.schemaHashes[type],"shared-slug",cfg)));
    require(rows[0].title==="Design only"&&rows[1].title==="Task only"&&rows[0].body!==rows[1].body,"Canonical field reuse shared record values");
  }
  return {logical_records:count,body_bytes:count*body(0).length,cursor_records:a.label==="candidate"?count:null};
}
function describe(values) {
  const sorted=[...values].sort((a,b)=>a-b),percentile=(p)=>sorted[Math.ceil(sorted.length*p)-1];
  return {requests:values.length,errors:0,p50_ms:percentile(.5),p95_ms:percentile(.95),p99_ms:percentile(.99)};
}
if(a.mode==="setup")await setup();
else if(a.mode==="measure") {
  const cfg=JSON.parse(readFileSync(a.config,"utf8")),node=await client(active,cfg);
  const membership=a.app==="fkanban"?await kanbanMeasure(node,cfg):await brainMeasure(node,cfg);
  const result={app:a.app,label:a.label,membership,metrics:Object.fromEntries(Object.entries(samples).map(([name,values])=>[name,describe(values)]))};
  writeFileSync(a.output+"/samples.json",JSON.stringify(samples)+"\n");
  writeFileSync(a.output+"/result.json",JSON.stringify(result,null,2)+"\n");
  console.log(JSON.stringify(result));
} else throw Error("Unknown benchmark mode");

