from __future__ import with_statement
import cPickle, os, xmlrpclib, time, urllib2, httplib, socket
from xml.etree import ElementTree
import xml.parsers.expat
import sqlite

# library config
pypi = 'pypi.python.org'
BASE = 'https://'+pypi
SIMPLE = BASE + '/simple/'
version = '1.5'
UA = 'pep381client-proxy/'+version

# Helpers

_proxy_name = None
_proxy_host = None
_proxy_port = None
_timeout = 30.0
def get_proxy():
    global _proxy_name
    global _proxy_host
    global _proxy_port
    if _proxy_name is None and os.environ.has_key('http_proxy'):
        _proxy_name = os.environ['http_proxy']
        _full_proxy_name = os.environ['http_proxy']
        print _proxy_name
        scheme = _proxy_name.find('//')
        if scheme >= 0:
            _proxy_name=_proxy_name[scheme+2:]
            
            hostIndex = _proxy_name.find(':')
            _proxy_host = _proxy_name[:hostIndex]
            _proxy_port = _proxy_name[hostIndex+1:]


class HTTP_with_timeout(httplib.HTTP):
    def __init__(self, host='', port=None, strict=None, timeout=30.0):
        if port == 0: port = None
        self._setup(self._connection_class(host, port, strict, timeout=timeout))

    def getresponse(self, *args, **kw):
        return self._conn.getresponse(*args, **kw)

# Transport Herlper code get from xmlrpclib documentation
class ProxiedTransport(xmlrpclib.Transport):
    def set_proxy(self, proxy):
        self.proxy = proxy
    def make_connection(self, host):
        self.realhost = host
        h = HTTP_with_timeout(self.proxy, timeout=self.timeout)
        return h
    def send_request(self, connection, handler, request_body):
        connection.putrequest("POST", 'https://%s%s' % (self.realhost, handler))
    def send_host(self, connection, host):
        connection.putheader('Host', self.realhost)



class ProxyHTTPConnection(httplib.HTTPConnection):
    """ Quick and Dirty Redefined HTTPConnection to get client work behind a proxy."""
    def set_proxy(self, proxy):
        self.set_tunnel(_proxy_host, _proxy_port)

    def connect(self):
        if not self._tunnel_host:
            self.sock = socket.create_connection((self.host,self.port), self.timeout)
        else:
            self.sock = socket.create_connection((self._tunnel_host,self._tunnel_port), self.timeout)

    def putrequest(self, method, url, skip_host=0, skip_accept_encoding=0):
        if self._tunnel_host:
            url="http://%s:%s%s" % (self.host, self.port, url)
        httplib.HTTPConnection.putrequest(self, method, url, skip_host, skip_accept_encoding)

_proxy = None
def xmlrpc():
    global _proxy
    if _proxy is None:
        get_proxy()
        if _proxy_name is None:
            _proxy = xmlrpclib.ServerProxy(BASE+'/pypi')
        else:
            pt = ProxiedTransport()
            pt.set_proxy(_proxy_name)
            _proxy = xmlrpclib.ServerProxy(BASE+'/pypi', transport=pt)
        _proxy.useragent = UA
    return _proxy

_conn = None
_opener = None
def http():
    global _opener
    global _conn    
    _opener = urllib2.build_opener()
    return _opener

def now():
    return int(time.time())

# Main class

class Synchronization:
    "Picklable status of a mirror"

    def __init__(self):
        self.homedir = None
        self.quiet = False
        
        # time stamps: seconds since 1970
        self.last_completed = 0 # when did the last run complete
        self.last_started = 0   # when did the current run start

        self.complete_projects = set()
        self.projects_to_do = set()

        self.skip_file_contents = False

    def defaults(self):
        # Fill fields that may not exist in the pickle
        for field, value in (('quiet', False),):
            if not hasattr(self, field):
                setattr(self, field, value)

    def store(self):
        with open(os.path.join(self.homedir, "status"), "wb") as f:
            cPickle.dump(self, f, cPickle.HIGHEST_PROTOCOL)
            self.storage.commit()

    @staticmethod
    def load(homedir, storage=None):
        res = cPickle.load(open(os.path.join(homedir, "status"), "rb"))
        res.storage = storage or sqlite.SqliteStorage(os.path.join(homedir, "files"))
        res.defaults()
        res.homedir = homedir # override pickled value in case it got relocated
        return res

    #################### Synchronization logic ##############################

    @staticmethod
    def initialize(targetdir, storage=None):
        'Create a new empty mirror. This operation should not be interrupted.'
        if not os.path.exists(targetdir):
            os.makedirs(targetdir)
        else:
            assert not os.listdir(targetdir)
        for d in ('simple', 'packages', 'serversig',
                  'local-stats/days'):
            os.makedirs(os.path.join(targetdir, 'web', d))
        status = Synchronization()
        status.homedir = targetdir
        status.last_started = now()
        status.projects_to_do = set(xmlrpc().list_packages())
        status.storage = storage or sqlite.SqliteStorage(os.path.join(status.homedir, "files"))
        status.store()
        return status

    def synchronize(self):
        # check whether another job is already running
        pid = self.storage.find_running()
        if pid:
            # check whether process still runs
            try:
                os.kill(pid, 0)
            except OSError:
                # process is gone, take over
                self.storage.end_running()
            else:
                self.storage.commit()
                if not self.quiet:
                    print "Currently already running; mirroring is skipped"
                return
        self.storage.start_running(os.getpid())
        self.storage.commit()
        try:
            self._synchronize()
        finally:
            self.storage.end_running()
            self.storage.commit()

    def _synchronize(self):
        'Run synchronization. Can be interrupted and restarted at any time.'
        if self.last_started == 0:
            # no synchronization in progress. Fetch changelog
            self.last_started = now()
            changes = xmlrpc().changelog(self.last_completed-1)
            if not changes:
                self.update_timestamp(self.last_started)
                return
            for change in changes:
                self.projects_to_do.add(change[0])
            self.copy_simple_page('')
            self.store()
        # sort projects to allow for repeatable runs
        for project in sorted(self.projects_to_do):
            print 'project = ' + project
            if not project:
                # skip empty project names resulting from PyPI-wide changes
                continue
            if project.encode('utf-8') in ['iterator', 'nester_test_ling', 'nesterswe']:
                continue
            if not self.quiet:
                print "Synchronizing", project.encode('utf-8')
            data = self.copy_simple_page(project)
            if not data:
                self.delete_project(project)
                self.store()
                continue
            try:
                files = set(self.get_package_files(data))
            except xml.parsers.expat.ExpatError, e:
                # not well-formed, skip for now
                if not self.quiet:
                    print "Page for %s cannot be parsed: %r" % (project, e)
                raise
            for file in files:
                if not self.quiet:
                    print "Copying", file
                self.maybe_copy_file(project, file)
            # files start with /; remove it
            relfiles = set(p[1:] for p in files)
            for file in self.storage.files(project)-relfiles:
                    self.remove_file(file)
            self.complete_projects.add(project)
            self.projects_to_do.remove(project)
            self.store()
        self.update_timestamp(self.last_started)
        self.last_completed = self.last_started
        self.last_started = 0
        self.store()

    def update_timestamp(self, when):
        with open(os.path.join(self.homedir, "web", "last-modified"), "wb") as f:
            f.write(time.strftime("%Y%m%dT%H:%M:%S\n", time.gmtime(when)))

    def copy_simple_page(self, project):
        project = project.encode('utf-8')
        opener = http()
        print "project is " + project
        status_code = None
        try:
            response = opener.open(BASE + '/simple/'+urllib2.quote(project)+'/')
            html = response.read()
        except urllib2.HTTPError, e:
            status_code = e.code
            print  "Error Code: " + str(e.code) + ', when copy simple page for project: ' + project
        except Exception, e:
            raise e
        
        if status_code == 404:
            return None
        if status_code == 301:
            # package not existant anymore, however, similarly-spelled
            # package exists
            return None
        if response.getcode() != 200:
            raise ValueError, "Status %d on %s" % status_code, project
        project_simple_dir = os.path.join(self.homedir, 'web', 'simple', project)
        if not os.path.exists(project_simple_dir):
            os.mkdir(project_simple_dir)
        with open(os.path.join(project_simple_dir, 'index.html'), "wb") as f:
            f.write(html)

        response2 = opener.open(BASE +'/serversig/'+urllib2.quote(project)+'/')
        #h.putrequest('GET', '/serversig/'+urllib2.quote(project)+'/', headers ={'User-Agent': UA})
        #r = h.getresponse()
        sig = response2.read()
        if response2.getcode() != 200:
            if not project:
                # index page is unsigned
                return
            raise ValueError, "Status %d on signature for %s" % (response2.getcode(), project)
        with open(os.path.join(self.homedir, "web", "serversig", project), "wb") as f:
            f.write(sig)
        return html

    def get_package_files(self, data):
        x = ElementTree.fromstring(data)
        res = []
        for a in x.findall(".//a"):
            url = a.attrib['href']
            if not url.startswith('../../packages/'):
                continue
            url = url.split('#')[0]
            url = url[len('../..'):]
            res.append(url)
        return res

    def maybe_copy_file(self, project, path):
        opener = http()
        response = None
        headers = {'User-Agent': UA}
        flag = True
        if self.skip_file_contents:
            flag = True
        else:           
            flag = False
        etag = self.storage.etag(path)
        if etag:
            headers.append("If-none-match", etag)
        request = urllib2.Request(BASE + urllib2.quote(path))
        if flag:
            request.get_method = lambda : 'HEAD'
        else:
            request.get_method = lambda : 'GET'

        for header, value in headers.iteritems():
            request.add_header(header, value)

        status_code = None
        response = None
        try:
            response = urllib2.urlopen(request)
            status_code = response.getcode()
        except urllib2.HTTPError, e:
            status_code = e.code
            print  "Error Code: " + str(e.code) + ', when maybe_copy_file ' + e
        except Exception, e:
            raise e
        
        if status_code == 304:
            # not modified, discard data
            #r.read()
            #response.read()
            return
        if path.startswith("/"):
            path = path[1:]
        lpath = os.path.join(self.homedir, "web", path)
        if status_code == 200:
            self.storage.remove_file(path) # readd when done downloading
            data = response.read()
            dirname = os.path.dirname(lpath)
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            with open(lpath, "wb") as f:
                f.write(data)
            # XXX may set last-modified timestamp on file
            tag = response.info().getheader("etag")
            if tag:
                self.storage.add_file(project, path, tag)
            self.store()
            return
        if status_code == 404:
            self.remove_file(path)

    def remove_file(self, path):
        self.storage.remove_file(path)
        lpath = os.path.join(self.homedir, "web", path)
        if os.path.exists(lpath):
            os.unlink(lpath)

    def delete_project(self, project):
        for f in self.storage.files(project):
            self.remove_file(f)
        project_simple_dir = os.path.join(self.homedir, "web", "simple", project)
        if os.path.exists(project_simple_dir):
            index_file = os.path.join(project_simple_dir, "index.html")
            if os.path.exists(index_file):
                os.unlink(index_file)
            os.rmdir(project_simple_dir)
        project_serversig_dir = os.path.join(self.homedir, "web", "serversig", project)
        if os.path.exists(project_serversig_dir):
            os.unlink(project_serversig_dir)
        if project in self.projects_to_do:
            self.projects_to_do.remove(project)
        if project in self.complete_projects:
            self.complete_projects.remove(project)
