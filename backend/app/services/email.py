import resend
from app.core.config import settings


def send_otp_email(otp: str, to_email: str) -> bool:
    """Send OTP to the specified email address via Resend."""
    resend.api_key = settings.RESEND_API_KEY
    try:
        params = {
            "from": settings.OTP_FROM_EMAIL,
            "to":   [to_email],
            "subject": "Quantedge Login OTP",
            "html": f"""
            <div style="font-family:monospace;background:#0a0a0a;color:#e5e5e5;padding:32px;border-radius:8px;max-width:400px;">
                <h2 style="color:#22d3ee;margin:0 0 16px">QUANTEDGE</h2>
                <p style="margin:0 0 8px;color:#a3a3a3;">Your one-time login code:</p>
                <div style="font-size:36px;font-weight:bold;letter-spacing:12px;color:#f0f9ff;margin:16px 0;">
                    {otp}
                </div>
                <p style="margin:16px 0 0;font-size:12px;color:#525252;">
                    Expires in 5 minutes. Do not share this code.
                </p>
            </div>
            """,
        }
        resend.Emails.send(params)
        return True
    except Exception as e:
        print(f"[Resend] Failed to send OTP to {to_email}: {e}")
        return False
