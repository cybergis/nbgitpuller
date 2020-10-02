from tornado import gen, web, locks
import traceback
import urllib.parse

from notebook.base.handlers import IPythonHandler
import threading
import json
import os
from queue import Queue, Empty
import jinja2
import logging

from .pull import GitPuller
from .version import __version__

## HydroShare
from hs_restclient import HydroShare, HydroShareAuthBasic, HydroShareAuthOAuth2
from urllib.parse import urljoin
from tornado.escape import url_escape, url_unescape
from .pull import HSPuller


class SyncHandler(IPythonHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # We use this lock to make sure that only one sync operation
        # can be happening at a time. Git doesn't like concurrent use!
        if 'git_lock' not in self.settings:
            self.settings['git_lock'] = locks.Lock()

    @property
    def git_lock(self):
        return self.settings['git_lock']

    @gen.coroutine
    def emit(self, data):
        if type(data) is not str:
            serialized_data = json.dumps(data)
            if 'output' in data:
                self.log.info(data['output'].rstrip())
        else:
            serialized_data = data
            self.log.info(data)
        self.write('data: {}\n\n'.format(serialized_data))
        yield self.flush()

    @web.authenticated
    @gen.coroutine
    def get(self):
        try:
            yield self.git_lock.acquire(1)
        except gen.TimeoutError:
            self.emit({
                'phase': 'error',
                'message': 'Another git operations is currently running, try again in a few minutes'
            })
            return

        try:
            repo = self.get_argument('repo')
            branch = self.get_argument('branch')
            depth = self.get_argument('depth', None)
            if depth:
                depth = int(depth)

            try:
                ## Drew 10/01/2020
                ## The request made to this endpoint is not orginated from user
                ## Instead, it is made from the template\status.html
                ## See: https://github.com/jupyterhub/nbgitpuller/blob/master/nbgitpuller/__init__.py#L20
                ## User request made to UIHandler, which parses and modifies url paratemers, and renders template\status.html
                ## template\status.html then makes requests to "git-pull/api?", handled by this SyncHandler
                ## see: https://github.com/jupyter/notebook/blob/0df10dee3ee7e8c0f56f879b69d37070fccca1c7/notebook/terminal/__init__.py#L37

                ## Env Var JUPYTER_SERVER_ROOT === c.NotebookApp.notebook_dir
                ## Change to use JUPYTER_SERVER_ROOT(notebook_dir) as the root folder for clone
                ## the final folder would be: JUPYTER_SERVER_ROOT + NBGITPULLER_PARENTPATH + RepoName (if parameter "targetpath" does NOT exist in url)
                ##                        or: JUPYTER_SERVER_ROOT + NBGITPULLER_PARENTPATH + targetpath

                # Env Var JUPYTER_SERVER_ROOT is not accessible here, so use nbapp obj
                notebook_dir = self.settings['nbapp'].notebook_dir
                repo_parent_dir = os.path.join(notebook_dir,
                                               os.getenv('NBGITPULLER_PARENTPATH', ''))

                repo_dir = os.path.join(repo_parent_dir, self.get_argument('targetpath', repo.split('/')[-1]))
                logging.warning("Using notebook_dir(JUPYTER_SERVER_ROOT) as root path for git clone: " + notebook_dir)
                logging.warning("Final full path for git clone: " + repo_dir)

            except:
                # The default working directory is the directory from which Jupyter
                # server is launched, which is not the same as the root notebook
                # directory assuming either --notebook-dir= is used from the
                # command line or c.NotebookApp.notebook_dir is set in the jupyter
                # configuration. This line assures that all repos are cloned
                # relative to server_root_dir/<optional NBGITPULLER_PARENTPATH>,
                # so that all repos are always in scope after cloning. Sometimes
                # server_root_dir will include things like `~` and so the path
                # must be expanded.
                repo_parent_dir = os.path.join(os.path.expanduser(self.settings['server_root_dir']),
                                               os.getenv('NBGITPULLER_PARENTPATH', ''))
                repo_dir = os.path.join(repo_parent_dir, self.get_argument('targetpath', repo.split('/')[-1]))

            # We gonna send out event streams!
            self.set_header('content-type', 'text/event-stream')
            self.set_header('cache-control', 'no-cache')

            gp = GitPuller(repo, branch, repo_dir, depth=depth, parent=self.settings['nbapp'])

            q = Queue()

            def pull():
                try:
                    for line in gp.pull():
                        q.put_nowait(line)
                    # Sentinel when we're done
                    q.put_nowait(None)
                except Exception as e:
                    q.put_nowait(e)
                    raise e
            self.gp_thread = threading.Thread(target=pull)

            self.gp_thread.start()

            while True:
                try:
                    progress = q.get_nowait()
                except Empty:
                    yield gen.sleep(0.5)
                    continue
                if progress is None:
                    break
                if isinstance(progress, Exception):
                    self.emit({
                        'phase': 'error',
                        'message': str(progress),
                        'output': '\n'.join([
                            line.strip()
                            for line in traceback.format_exception(
                                type(progress), progress, progress.__traceback__
                            )
                        ])
                    })
                    return

                self.emit({'output': progress, 'phase': 'syncing'})

            self.emit({'phase': 'finished'})
        except Exception as e:
            self.emit({
                'phase': 'error',
                'message': str(e),
                'output': '\n'.join([
                    line.strip()
                    for line in traceback.format_exception(
                        type(e), e, e.__traceback__
                    )
                ])
            })
        finally:
            self.git_lock.release()


class UIHandler(IPythonHandler):
    def initialize(self):
        super().initialize()
        # FIXME: Is this really the best way to use jinja2 here?
        # I can't seem to get the jinja2 env in the base handler to
        # actually load templates from arbitrary paths ugh.
        jinja2_env = self.settings['jinja2_env']
        jinja2_env.loader = jinja2.ChoiceLoader([
            jinja2_env.loader,
            jinja2.FileSystemLoader(
                os.path.join(os.path.dirname(__file__), 'templates')
            )
        ])

    @web.authenticated
    @gen.coroutine
    def get(self):
        app_env = os.getenv('NBGITPULLER_APP', default='notebook')

        repo = self.get_argument('repo')
        branch = self.get_argument('branch', 'master')
        depth = self.get_argument('depth', None)
        urlPath = self.get_argument('urlpath', None) or \
                  self.get_argument('urlPath', None)
        subPath = self.get_argument('subpath', None) or \
                  self.get_argument('subPath', '.')
        app = self.get_argument('app', app_env)
        parent_reldir = os.getenv('NBGITPULLER_PARENTPATH', '')
        # targetpath = self.get_argument('targetpath', None) or \
        #              self.get_argument('targetPath', repo.split('/')[-1])

        ## Add url parameter "subfolder" --- Drew 10/01/2020
        ## if "targetpath" or "targetPath" does not exist
        ## set targetpath = subfolder + repo.split('/')[-1]
        targetpath = self.get_argument('targetpath', None) or self.get_argument('targetPath', None)
        if targetpath is None:
            targetpath = os.path.join(self.get_argument('subfolder', ''),
                                      repo.split('/')[-1])
        logging.warning("targetpath is updated: " + targetpath)

        ## urlPath is the url users get redircted to after git clone
        if urlPath:
            path = urlPath
        else:
            path = os.path.join(parent_reldir, targetpath, subPath)
            if app.lower() == 'lab':
                path = 'lab/tree/' + path
            elif path.lower().endswith('.ipynb'):
                path = 'notebooks/' + path
            else:
                path = 'tree/' + path

        logging.warning("Will redirect user after git clone: " + path)

        self.write(
            self.render_template(
                'status.html',
                repo=repo, branch=branch, path=path, depth=depth, targetpath=targetpath, version=__version__
            ))
        self.flush()


class LegacyGitSyncRedirectHandler(IPythonHandler):
    @web.authenticated
    @gen.coroutine
    def get(self):
        new_url = '{base}git-pull?{query}'.format(
            base=self.base_url,
            query=self.request.query
        )
        self.redirect(new_url)


class LegacyInteractRedirectHandler(IPythonHandler):
    @web.authenticated
    @gen.coroutine
    def get(self):
        repo = self.get_argument('repo')
        account = self.get_argument('account', 'data-8')
        repo_url = 'https://github.com/{account}/{repo}'.format(account=account, repo=repo)
        query = {
            'repo': repo_url,
            # branch & subPath are optional
            'branch': self.get_argument('branch', 'gh-pages'),
            'subPath': self.get_argument('path', '.')
        }
        new_url = '{base}git-pull?{query}'.format(
            base=self.base_url,
            query=urllib.parse.urlencode(query)
        )

        self.redirect(new_url)


########### HydroShare #########

class HSLoginHandler(IPythonHandler):
    @gen.coroutine
    def get(self):
        self.log.info('LOGIN GET' + self.request.uri)
        params = {
            "hslogin": urljoin(self.request.uri, 'hslogin'),
            "image": urljoin(self.request.uri, 'hs-pull/static/hydroshare_logo.png'),
            "error": self.get_argument("error", 'Login Needed'),
            "next": self.get_argument("next", "/")
        }
        temp = self.render_template("hslogin.html", **params)
        self.write(temp)

    @gen.coroutine
    def post(self):
        pwfile = os.path.expanduser("~/.hs_pass")
        userfile = os.path.expanduser("~/.hs_user")
        with open(userfile, 'w') as f:
            f.write(self.get_argument("name"))
        with open(pwfile, 'w') as f:
            f.write(self.get_argument("pass"))
        self.redirect(url_unescape(self.get_argument("next", "/")))


# Handle request from hstatus.html
class HSyncHandler(IPythonHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log.info("HSyncHandler")

    @gen.coroutine
    def emit(self, data):
        if type(data) is not str:
            serialized_data = json.dumps(data)
            if 'output' in data:
                self.log.info(data['output'].rstrip())
        else:
            serialized_data = data
            self.log.info(data)
        self.write('data: {}\n\n'.format(serialized_data))
        yield self.flush()

    @gen.coroutine
    def post(self):
        print(self.get_argument("email"), self.get_argument("name"))
        print('id=', self.get_argument('id'))

    # @web.authenticated
    @gen.coroutine
    def get(self):
        try:
            id = self.get_argument('id')
            download_folder_path = self.get_argument('download_folder_path', None)
            if download_folder_path is None:
                download_folder_path = os.path.join(
                                         self.settings['nbapp'].notebook_dir,
                                         os.getenv('NBGITPULLER_PARENTPATH', ''),
                                         self.get_argument('subfolder', ''))

            # We gonna send out event streams!
            self.set_header('content-type', 'text/event-stream')
            self.set_header('cache-control', 'no-cache')

            hs = HSPuller(id, self.settings['hydroshare'], download_folder_path)

            q = Queue()

            def pull():
                try:
                    for line in hs.pull():
                        q.put_nowait(line)
                    # Sentinel when we're done
                    q.put_nowait(None)
                except Exception as e:
                    q.put_nowait(e)
                    raise e

            self.hs_thread = threading.Thread(target=pull)

            self.hs_thread.start()

            while True:
                try:
                    progress = q.get_nowait()
                except Empty:
                    yield gen.sleep(0.5)
                    continue
                if progress is None:
                    break
                if isinstance(progress, Exception):
                    self.emit({
                        'phase': 'error',
                        'message': str(progress),
                        'output': '\n'.join([
                            l.strip()
                            for l in traceback.format_exception(
                                type(progress), progress, progress.__traceback__
                            )
                        ])
                    })
                    return

                self.emit({'output': progress, 'phase': 'syncing'})

            self.emit({'phase': 'finished'})
        except Exception as e:
            self.emit({
                'phase': 'error',
                'message': str(e),
                'output': '\n'.join([
                    l.strip()
                    for l in traceback.format_exception(
                        type(e), e, e.__traceback__
                    )
                ])
            })


# handel user request and render hstatus.html
class HSHandler(IPythonHandler):
    def initialize(self):
        super().initialize()
        jinja2_env = self.settings['jinja2_env']
        jinja2_env.loader = jinja2.ChoiceLoader([
            jinja2_env.loader,
            jinja2.FileSystemLoader(
                os.path.join(os.path.dirname(__file__), 'templates')
            )
        ])

    def check_auth(self, auth):
        hs = HydroShare(auth=auth)
        self.log.info('hs=%s' % str(hs))

        try:
            info = hs.getUserInfo()
            self.settings['hydroshare'] = hs
            self.log.info('info=%s' % info)
        except:
            hs = None
        return hs

    def login(self):
        hs = None

        # check for oauth
        authfile = os.path.expanduser("~/work/.hs_auth")
        try:
            hs_auth_str = os.getenv("HS_AUTH", None)
            if hs_auth_str is None:
                if os.path.isfile(authfile):
                    with open(authfile, "r") as f:
                        hs_auth = json.load(f)

                        # set environ in python only works within this python session
                        os.environ["HS_AUTH"] = json.dumps(hs_auth)
                else:
                    self.log.error("No HS_AUTH on env or .hs_auth in ~/work")
                    raise Exception("No HS_AUTH on env or .hs_auth in ~/work")
            else:
                hs_auth = json.loads(hs_auth_str)
                try:
                    with open(authfile, "w") as f:
                        f.write(hs_auth_str)
                    os.system('chmod -R 777 {}'.format(authfile))
                except Exception as ex:
                    self.log.warn("Cant write Token from ENV to file")

            self.log.info("HS_AUTH from JupyterHub: {}".format(hs_auth))
            token, cid = hs_auth

            # Hard code for testing purpose
            # token = {'access_token': 'o7hAvxqNzLZiehrXXXXXXXXXXX',
            #          'token_type': 'Bearer',
            #          'expires_in': 2592000,
            #          'refresh_token': 'cEBqQ4wPCXFNQXXXXXXXXXXX',
            #          'scope': 'read write'}
            # cid = "cWqnyXCcA2h01vmwaX3E21XXXXXXXXXXXXX"

            auth = HydroShareAuthOAuth2(cid, '', token=token)
            self.log.info('auth=%s' % str(auth))

            hs = self.check_auth(auth)
            self.log.info('hs=%s' % str(hs))

            if hs is None:
                message = url_escape(
                    "Oauth Login Failed.  Login with username and password or logout from JupyterHub and reauthenticate with Hydroshare.")
        except Exception as ex:
            self.log.error(ex)
            message = ''

        if hs is None:
            # If oauth fails, we can log in using
            # user name and password.  These are saved in
            # files in the home directory.
            pwfile = os.path.expanduser("~/.hs_pass")
            userfile = os.path.expanduser("~/.hs_user")

            try:
                with open(userfile) as f:
                    username = f.read().strip()
                with open(pwfile) as f:
                    password = f.read().strip()
                auth = HydroShareAuthBasic(username=username, password=password)
                hs = self.check_auth(auth)
                if hs is None:
                    message = url_escape("Login Failed. Please Try again")
            except:
                message = url_escape("You need to provide login credentials to access HydroShare Resources.")

        if hs is None:
            _next = url_escape(url_escape(self.request.uri))
            upath = urljoin(self.request.uri, 'hslogin')
            self.redirect('%s?error=%s&next=%s' % (upath, message, _next))

    @web.authenticated
    @gen.coroutine
    def get(self):
        """
        Get or go to the required resource. Parameters:
        id - Resource id (required)
        start - notebook to launch (optional)
        app - Optinal. 'lab' will try to run jupyter lab
        overwrite - Overwrite any existing local copy or the resource
        goto - Do not overwrite.  Just go to the resouce
        """

        app_env = os.getenv('NBGITPULLER_APP', default='notebook')
        self.log.info('HS GET ' + str(self.request.uri))

        self.login()

        id = self.get_argument('id')
        start = self.get_argument('start', '')
        app = self.get_argument('app', app_env)
        overwrite = self.get_argument('overwrite', 0)
        goto = self.get_argument('goto', 0)

        self.log.info('GET %s %s %s %s %s' % (id, start, app, overwrite, goto))

        # create Downloads directory if necessary

        ##download_dir = os.environ.get('JUPYTER_DOWNLOADS', 'work/Downloads')
        download_root = self.settings['nbapp'].notebook_dir
        download_relative_path = os.path.join(
                                         os.getenv('NBGITPULLER_PARENTPATH', ''),
                                         self.get_argument('subfolder', ''))
        download_folder_path = os.path.join(download_root, download_relative_path)

        if not os.path.isdir(download_folder_path):
            os.makedirs(download_folder_path)

        ## nbdir = os.environ.get('NOTEBOOK_HOME')
        ## relative_download_dir = os.path.relpath(download_dir, nbdir)
        resource_folder = os.path.join(download_folder_path, id)
        if os.path.exists(resource_folder) and goto == 0 and overwrite == 0:
            # overwrite or not? display modal dialog
            self.write(self.render_template('confirm.html', directory=download_relative_path))
            self.flush()
            return

        redirect_rel_path = os.path.join(download_relative_path, id, 'data', 'contents', start)

        if app.lower() == 'lab':
            redirect_rel_path = 'lab/tree/' + redirect_rel_path
        elif redirect_rel_path.lower().endswith('.ipynb'):
            redirect_rel_path = 'notebooks/' + redirect_rel_path
        else:
            redirect_rel_path = 'tree/' + redirect_rel_path

        self.log.info('redirect_rel_path=%s' % redirect_rel_path)
        if goto:
            self.redirect(redirect_rel_path)
            return

        self.write(
            self.render_template(
                'hstatus.html',
                id=id,
                redirect_rel_path=redirect_rel_path,
                version=__version__,
                download_folder_path=download_folder_path,
            ))
        self.flush()
