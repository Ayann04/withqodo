from django.db import models
from django.utils import timezone

class ScrapingRun(models.Model):
    started_at = models.DateTimeField(default=timezone.now)
    finished_at = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return f"Run {self.id} - {self.started_at.strftime('%Y-%m-%d %H:%M:%S')}"
    

class ScrapingStatus(models.Model):
    run = models.ForeignKey(
        ScrapingRun,
        on_delete=models.CASCADE,
        related_name="statuses",
        null=True,   # 👈 allow empty for old rows
        blank=True
    )
    message = models.CharField(max_length=255, default="No Message")
    created_at = models.DateTimeField(default=timezone.now)
    captcha_image = models.ImageField(upload_to="captchas/", null=True, blank=True)  
    
class ScrapedRecord(models.Model):
    # You can store each section as JSON to keep it flexible
    registration_details = models.JSONField(blank=True, null=True)
    seller_details = models.JSONField(blank=True, null=True)
    buyer_details = models.JSONField(blank=True, null=True)
    property_details = models.JSONField(blank=True, null=True)
    khasra_details = models.JSONField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Record {self.id} - {self.created_at.strftime('%Y-%m-%d %H:%M:%S')}"
