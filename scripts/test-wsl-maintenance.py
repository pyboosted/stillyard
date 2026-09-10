"""SQLite/journal maintenance controls; run as a default Stillyard Job."""
import copy,json,sqlite3,unittest,uuid
from wsl_maintenance import boundary_digest,sql_barrier

class Maintenance(unittest.TestCase):
    def setUp(self):
        self.store,self.inv,self.cid=[str(uuid.uuid4()) for _ in range(3)]
        self.key=self.store+'~'+self.inv
        self.record={'containment':self.store+'~'+self.cid,'lease':str(uuid.uuid4()),
                     'daemon_generation':str(uuid.uuid4()),'creator':{'platform':'linux','pid':123},
                     'boundary':{'path':'/isolated/fixture','inode':123},'root':{'pid':456},
                     'executable_sha256':'a'*64,'release_intent':{'fixture':True}}
        self.record['seal']={'invocation':self.key,'boundary_sha256':boundary_digest(self.record),
                             'seal_id':str(uuid.uuid4()),'possibly_released':True}
        self.records={self.key:self.record}
        self.audit={'resolution':'proven_empty','last_reconciliation':'proven_empty',
                    'origin':'automatic','forced':None,'lease_released':True,
                    'resolved_unix_millis':12345,'daemon_generation':str(uuid.uuid4())}
        self.db=sqlite3.connect(':memory:',isolation_level=None)
        self.db.executescript('create table leases(state text); create table containments(id text,invocation_id text,state text,resolution text,resolution_audit_json text);')
        self.db.execute('insert into containments values(?,?,?,?,?)',(self.cid,self.inv,'cleared','proven_empty',json.dumps(self.audit)))
        self.db.execute('BEGIN IMMEDIATE')
    def tearDown(self):self.db.close()
    def check(self,records=None):return sql_barrier(self.db,self.store,self.records if records is None else records)
    def test_accepts_matching_automatic_seal_without_changing_history(self):
        before=self.db.execute('select * from containments').fetchall()
        result=self.check();self.assertEqual(len(result['historical_proven_empty']),1)
        self.assertEqual(self.db.execute('select * from containments').fetchall(),before)
        self.assertTrue(self.db.in_transaction)
    def test_requires_admission_transaction_and_no_active_lease(self):
        self.db.rollback()
        with self.assertRaisesRegex(RuntimeError,'admission transaction'):self.check()
        self.db.execute('BEGIN IMMEDIATE');self.db.execute("insert into leases values('granted')")
        with self.assertRaisesRegex(RuntimeError,'Lease'):self.check()
    def test_rejects_live_uncertain_forced_or_unaudited_rows(self):
        for state in ('live','uncertain','prepared'):
            with self.subTest(state=state):
                self.db.execute('update containments set state=?',(state,))
                with self.assertRaises(RuntimeError):self.check()
        self.db.execute("update containments set state='cleared'")
        for field,value in [('resolution','forced_risk_acceptance'),('last_reconciliation','unknown'),
                            ('origin','operator'),('forced',{}),('lease_released',False),('daemon_generation','invalid')]:
            with self.subTest(field=field):
                audit=self.audit|{field:value};self.db.execute('update containments set resolution_audit_json=?',(json.dumps(audit),))
                with self.assertRaises(RuntimeError):self.check()
        for value in (None,'{corrupt','{}'):
            self.db.execute('update containments set resolution_audit_json=?',(value,))
            with self.assertRaises(RuntimeError):self.check()
    def test_rejects_absent_foreign_unsealed_or_rebound_journal_records(self):
        with self.assertRaises(RuntimeError):self.check({})
        for field,value in [('containment',self.store+'~'+str(uuid.uuid4())),('boundary',None),('root',{'pid':999})]:
            with self.subTest(field=field):
                record=copy.deepcopy(self.record);record[field]=value
                with self.assertRaises(RuntimeError):self.check({self.key:record})
        for field,value in [('invocation',str(uuid.uuid4())+'~'+self.inv),('boundary_sha256','b'*64),('seal_id',str(uuid.UUID(int=0))),('possibly_released',False)]:
            with self.subTest(seal_field=field):
                record=copy.deepcopy(self.record);record['seal'][field]=value
                with self.assertRaises(RuntimeError):self.check({self.key:record})
        record=copy.deepcopy(self.record);record['seal']=None
        with self.assertRaises(RuntimeError):self.check({self.key:record})


class SealedPending(unittest.TestCase):
    def setUp(self):
        self.store,self.inv,self.cid,self.lease,self.attempt,self.job=[str(uuid.uuid4()) for _ in range(6)]
        self.key=self.store+'~'+self.inv
        self.allocation={'manager_store_uuid':self.store,'lease_id':self.lease}
        self.record={'containment':self.store+'~'+self.cid,'lease':self.lease,
            'daemon_generation':str(uuid.uuid4()),'creator':{'pid':1},'boundary':{'inode':123},
            'root':{'pid':2},'executable_sha256':'a'*64}
        boundary=boundary_digest(self.record)
        ticket={'key':self.allocation,'intent':{'invocation_id':self.key,'containment_id':self.record['containment'],
            'boundary_sha256':boundary,'release_sequence':1}}
        self.record['release_intent']=ticket
        seal={'invocation':self.key,'boundary_sha256':boundary,'seal_id':str(uuid.uuid4()),'possibly_released':True}
        self.record['seal']=seal
        import hashlib
        proof=hashlib.sha256(json.dumps(seal,separators=(',',':')).encode()).hexdigest()
        cleanup={'invocation_id':self.key,'release_sequence':1,'boundary_sha256':boundary,'proof_sha256':proof,'user_code_released':True}
        self.records={self.key:self.record}
        self.db=sqlite3.connect(':memory:',isolation_level=None)
        self.db.executescript('''create table leases(id text,attempt_id text,invocation_id text,state text);
            create table jobs(id text,state text);
            create table attached_local_plans(lease_id text,job_id text,attempt_id text,allocation_key text,armed int,committed int,release_pending int,released int);
            create table invocations(id text,attempt_id text,role text,state text);
            create table containments(id text,invocation_id text,state text,resolution text,resolution_audit_json text);
            create table attached_local_cleanup(invocation_id text,boundary_sha256 text,proof_sha256 text);
            create table attached_tickets(invocation_id text,allocation_key text,ticket_json text,consumed int,cleanup_json text);''')
        self.db.execute('insert into leases values(?,?,?,?)',(self.lease,self.attempt,None,'granted'))
        self.db.execute('insert into jobs values(?,?)',(self.job,'final'))
        self.db.execute('insert into attached_local_plans values(?,?,?,?,1,1,1,0)',(self.lease,self.job,self.attempt,json.dumps(self.allocation)))
        self.db.execute('insert into invocations values(?,?,?,?)',(self.inv,self.attempt,'primary','resolved'))
        self.db.execute('insert into containments values(?,?,?,?,?)',(self.cid,self.inv,'empty',None,None))
        self.db.execute('insert into attached_local_cleanup values(?,?,?)',(self.key,boundary,proof))
        self.db.execute('insert into attached_tickets values(?,?,?,1,?)',(self.key,json.dumps(self.allocation),json.dumps(ticket),json.dumps(cleanup)))
        self.db.execute('BEGIN IMMEDIATE')
    def tearDown(self):self.db.close()
    def check(self):return sql_barrier(self.db,self.store,self.records,allow_sealed_release_pending=True)
    def test_retains_history_and_requires_explicit_opt_in(self):
        before=list(self.db.iterdump())
        with self.assertRaisesRegex(RuntimeError,'Lease'):sql_barrier(self.db,self.store,self.records)
        result=self.check();self.assertEqual(result['granted_leases'],1)
        self.assertEqual(result['sealed_release_pending'][0]['lease_id'],self.lease)
        self.assertEqual(list(self.db.iterdump()),before)
    def test_rejects_live_or_incomplete_relational_state(self):
        for mutation in ["update jobs set state='running'", "update leases set invocation_id='probe'",
                "update attached_local_plans set committed=0", "update attached_local_plans set release_pending=0",
                "update attached_local_plans set armed=0", "update attached_local_plans set released=1",
                "update attached_local_plans set attempt_id='foreign'", "delete from attached_local_plans",
                "update invocations set state='running'", "update containments set state='uncertain'",
                "delete from containments", "delete from attached_local_cleanup", "delete from attached_tickets",
                "update attached_tickets set consumed=0", "update attached_tickets set cleanup_json=null",
                "update attached_local_cleanup set proof_sha256='foreign'"]:
            with self.subTest(mutation=mutation):
                self.db.execute('SAVEPOINT control');self.db.execute(mutation)
                with self.assertRaises(RuntimeError):self.check()
                self.db.execute('ROLLBACK TO control');self.db.execute('RELEASE control')
    def test_rejects_false_or_rebound_seals(self):
        original=copy.deepcopy(self.records)
        for field,value in [('seal',None),('lease',str(uuid.uuid4())),('root',{'pid':999}),
                ('release_intent',None),('containment',self.store+'~'+str(uuid.uuid4()))]:
            with self.subTest(field=field):
                self.records=copy.deepcopy(original);self.records[self.key][field]=value
                with self.assertRaises(RuntimeError):self.check()
        for field,value in [('possibly_released',False),('seal_id',str(uuid.UUID(int=0))),
                ('boundary_sha256','b'*64),('invocation','foreign')]:
            with self.subTest(seal_field=field):
                self.records=copy.deepcopy(original);self.records[self.key]['seal'][field]=value
                with self.assertRaises(RuntimeError):self.check()

if __name__=='__main__':unittest.main()
