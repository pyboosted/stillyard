"""Recovery-build provenance controls; run as a default Stillyard Python Job."""
import copy,importlib.util,os,sys,types,unittest,uuid
from pathlib import Path
from unittest.mock import patch
if sys.platform=='win32':sys.modules['fcntl']=types.ModuleType('fcntl')
spec=importlib.util.spec_from_file_location('upgrade',Path(__file__).with_name('upgrade-wsl-daemon.py'))
upgrade=importlib.util.module_from_spec(spec);spec.loader.exec_module(upgrade)

class BootstrapBuild(unittest.TestCase):
    def setUp(self):
        home = patch.object(Path, 'home', return_value=Path.cwd() / 'fixture-owner')
        home.start(); self.addCleanup(home.stop)
        self.env=patch.dict(os.environ,{'WSL_DISTRO_NAME':'isolated-fixture'});self.env.start();self.addCleanup(self.env.stop)
        self.job=str(uuid.uuid4())+'~'+str(uuid.uuid4());self.operation=str(uuid.uuid4())
        self.candidate=(Path.cwd()/'target/release/stillyard').resolve()
        self.build={'state':'final','outcome':'succeeded','spec':{'labels':[{'key':'gate','value':'wsl-bootstrap-build-release'}],
            'args':['bootstrap','run','--spec','retained-work.json'],'resources':{'cargo_slots':1}}}
        self.hold={'released':True,'bootstrap':{'parent':{'job_id':self.job,'invocation_id':self.job.split('~')[0]+'~'+self.operation},
            'request_sha256':'a'*64,'work':{'operation_id':self.operation,'args':['build','--locked','--release'],
                'executable':str(Path.home()/'.cargo/bin/cargo'),'distribution':'isolated-fixture','user':Path.home().name,
                'environment':{'CARGO_TARGET_DIR':str(self.candidate.parent.parent)}}},
            'cleanup_proof':{'operation_id':self.operation,'request_sha256':'a'*64,'phase':'sealed_empty','root_exit_code':0,'termination':'exited'}}
    def check(self,holds=None):return upgrade.bootstrap_build(self.build,[self.hold] if holds is None else holds,self.job,self.candidate)
    def test_accepts_actual_work_and_seal_binding(self):self.assertEqual(self.check(),self.hold)
    def test_rejects_missing_duplicate_foreign_or_unsealed_holds(self):
        with self.assertRaises(RuntimeError):self.check([])
        with self.assertRaises(RuntimeError):self.check([self.hold,self.hold])
        for path,value in [(('released',),False),(('cleanup_proof',),None),
                (('bootstrap','parent','job_id'),'foreign'),(('bootstrap','work','args'),['check']),
                (('bootstrap','work','distribution'),'foreign'),(('bootstrap','work','user'),'foreign'),
                (('cleanup_proof','phase'),'uncertain'),(('cleanup_proof','root_exit_code'),1),
                (('cleanup_proof','request_sha256'),'b'*64),(('cleanup_proof','operation_id'),str(uuid.uuid4())),
                (('bootstrap','parent','invocation_id'),self.job)]:
            with self.subTest(path=path):
                hold=copy.deepcopy(self.hold);node=hold
                for part in path[:-1]:node=node[part]
                node[path[-1]]=value
                with self.assertRaises(RuntimeError):self.check([hold])
    def test_rejects_unsuccessful_job_and_different_candidate(self):
        self.build['outcome']='failed'
        with self.assertRaises(RuntimeError):self.check()
        self.build['outcome']='succeeded';self.candidate=self.candidate.with_name('other')
        with self.assertRaises(RuntimeError):self.check()

if __name__=='__main__':unittest.main()
