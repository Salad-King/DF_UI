from django import forms
from django.forms import formset_factory

class NameForm(forms.Form):
	your_name = forms.CharField(label='Your Name', max_length = 20)

class JobForm(forms.Form):
	queryPath = forms.CharField(label='Query Path:')


class ParamsForm(forms.Form):
	def __init__(self,request,*args,**kwargs):
		super(ParamsForm, self).__init__(*args, **kwargs)
		self.request = request
		try:
			for each_id in request.session.keys():
				self.fields[each_id] = forms.CharField(label=request.session[each_id],required=False)
		except:
			return
