import uuid
import re
import google.auth

from django.http import HttpResponse
from django.http import HttpResponseRedirect
from django.shortcuts import render,redirect
from googleapiclient.discovery import build
from google.cloud import storage
from datetime import datetime,timedelta

from .forms import NameForm,JobForm,ParamsForm

conditionRegex = re.compile(r"\w+ *[=!<>]+ *['\"]?%s['\"]?|\w+ *\w+ *['\"]?%s['\"]?")
splitRegex = re.compile(r'[ =<>!]+')

scopes = ['https://www.googleapis.com/auth/compute',
          'https://www.googleapis.com/auth/cloud-platform',
          'https://www.googleapis.com/auth/userinfo.email',
          'https://www.googleapis.com/auth/compute.readonly']

credentials = credentials,project = google.auth.default(scopes)
# Create your views here.
def hello(request):
    today = datetime.date.today()
    return render(request, "hello.html", {"today" : today})


def get_name(request):
    count = 1
    if request.method == 'POST':
        form = JobForm(request.POST)
        if form.is_valid():
            queryPath = form.cleaned_data['queryPath']
            client = storage.Client()
            bucket = client.get_bucket('aditi_airflow')
            blob = bucket.get_blob(queryPath)
            if blob == None:
                return HttpResponse("<h1>"+"404! FILE NOT FOUND"+"</h1>")
            query = (blob.download_as_string())
            for each in conditionRegex.findall(query.decode('utf-8')):
                data = splitRegex.split(each)
                if len(data)==3:
                    param = data[0]+' '+data[1]
                    request.session[param+str(count)] = param
                    count += 1
                elif len(data)==2:
                    res = (splitRegex.search(each))
                    request.session[data[0]+res.group().strip()+str(count)] = data[0]+res.group().strip()
        return HttpResponseRedirect("/form/params")
    else:
        form = JobForm()
        return render(request,"form.html",{'form' : form})


def get_params(request):
    global credentials
    dataflow = build('dataflow', 'v1b3', credentials=credentials)
    if request.method == 'POST':
        form = ParamsForm(request,request.POST)
        if form.is_valid():
            params = 'gs://aditi_airflow/Query.txt,'
            for each in form.cleaned_data.keys():
                if len(form.cleaned_data[each]) == 0 or form.cleaned_data[each] == None:
                    params +=  ' |'
                else:
                    params += (form.cleaned_data[each] + '|')
            BODY = {
                "jobName": "{jobname}".format(jobname='query-job'+str(uuid.uuid1())[:8]),
                "parameters": {
                   "inputFile": "{trigger_file}".format(trigger_file=params[0:len(params)-1]),
                 }
            }
            request = dataflow.projects().templates().launch(projectId='analytics-and-presentation', gcsPath='gs://aarti_template/sqlPOC', body=BODY)
            response = request.execute()

            return redirect('inner_home')
        else:
            return HttpResponse("<h1>Invalid request</h1>")
    else:
        form = ParamsForm(request)
        return render(request, "params.html", {'form': form});

def home(request):
    return render(request, "home.html")

def frame_home(request):
    return render(request, "frame_home.html")

def monitor(request):
    global  credentials
    dataflow = build('dataflow', 'v1b3', credentials=credentials)
    items = []
    count = 1
    ob = dataflow.projects().jobs().list(projectId='analytics-and-presentation')
    res = ob.execute()
    for each in res['jobs']:
        if datetime.strptime(each['currentStateTime'],"%Y-%m-%dT%H:%M:%S.%fZ") > (datetime.utcnow() - timedelta(hours=4)):
            each['currentStateTime'] = str(datetime.strptime(each['currentStateTime'],"%Y-%m-%dT%H:%M:%S.%fZ"))
            each['num'] = count
            count += 1
            items.append(each)
    if len(items) > 0:
        return render(request, "monitor.html", {'jobs': items})
    else:
        return HttpResponse('<h2>No Jobs on Dataflow Page</h2>')



