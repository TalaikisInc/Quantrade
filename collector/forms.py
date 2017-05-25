from django import forms
from django.utils.translation import ugettext as T
from django.utils.safestring import mark_safe

from .models import (Brokers, Portfolios, QtraUser)


class ContactForm(forms.Form):
    name = forms.CharField(max_length=40, label='Name')
    sender = forms.EmailField(required=True, label='Email')
    subject = forms.CharField(max_length=100, required=True, label='Subject')
    message = forms.CharField(widget=forms.Textarea, required=True, label='Message')
    #cc_myself = forms.BooleanField(required=False)


class ContactFormUser(forms.Form):
    subject = forms.CharField(max_length=100, required=True, label='Subject')
    message = forms.CharField(widget=forms.Textarea, required=True, label='Message')


class BrokerForm(forms.Form):
    broker = forms.ModelChoiceField(queryset=Brokers.objects.all(), label=T('Broker:'), required=True)
    our_links = forms.BooleanField(required=True, widget=forms.CheckboxInput, label=mark_safe(T('I have opened live<br />account using <strong>your links</strong>:')))
    account_id = forms.IntegerField(max_value=999999999, required=True, widget=forms.NumberInput, label=T('My account number:'))


class EmailForm(forms.ModelForm):
    class Meta:
        model = QtraUser
        fields = ['email']
