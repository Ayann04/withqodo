# myapp/urls.py
from django.urls import path
from . import views
from django.conf import settings
from django.conf.urls.static import static


urlpatterns = [
    path('', views.trigger_scrape, name='trigger_scrape'),
    path("get-status/", views.get_status, name="get_status"),
    path("clear-logs/", views.clear_logs, name="clear_logs"),
    path('download/', views.download_excel, name='download_excel'),
]
if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL,document_root=settings.MEDIA_ROOT)
    
    
    
    
