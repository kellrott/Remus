from django.conf.urls.defaults import patterns, include, url

# Uncomment the next two lines to enable the admin:
# from django.contrib import admin
# admin.autodiscover()

urlpatterns = patterns('',
    # Examples:
    # url(r'^$', 'remusWeb.views.home', name='home'),
    # url(r'^remusWeb/', include('remusWeb.foo.urls')),

    # Uncomment the admin/doc line below to enable admin documentation:
    # url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    # url(r'^admin/', include(admin.site.urls)),
    url(r'^$', 'dataView.views.home'),
    url(r'^(?P<instance>[\w\-]+)$', 'dataView.views.instance'),
    url(r'^(?P<instance>[\w\-]+)/(?P<table>[^:]*$)', 'dataView.views.table'),
    url(r'^(?P<instance>[\w\-]+)/(?P<table>[^:]*):(?P<key>.*$)', 'dataView.views.key')

)
