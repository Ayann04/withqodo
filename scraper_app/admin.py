from django.contrib import admin
from .models import ScrapingStatus,ScrapedRecord

admin.site.register(ScrapingStatus)
@admin.register(ScrapedRecord)
class ScrapedRecordAdmin(admin.ModelAdmin):
    list_display = ('id', 'created_at')
    


    
