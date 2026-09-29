import os
import html
import logging
import resend 

resend.api_key = os.environ.get("RESEND_API_KEY")
EMAIL_FROM = os.environ.get("EMAIL_FROM", "DineIQ <noreply@albstudio.dev>")
FRONTEND_URL = os.environ.get("FRONTEND_URL", "https://dineiq.albstudio.dev")

logger = logging.getLogger(__name__)

def send_onboarding_email(name: str, email: str, temp_password: str):
    login_url = f"{FRONTEND_URL}/login"
    safe_name = html.escape(name)
    safe_email = html.escape(email)
    safe_password = html.escape(temp_password)
    safe_login_url = html.escape(login_url, quote=True)

    body = f"""
    <div style="margin:0;padding:32px 16px;background:#f5f7f4;font-family:Inter,'Segoe UI',Arial,sans-serif;color:#17322a">
      <div style="max-width:580px;margin:0 auto;background:#ffffff;border:1px solid #e4ebe7;border-radius:20px;overflow:hidden;box-shadow:0 14px 40px rgba(24,54,44,.08)">
        <div style="padding:26px 30px;background:linear-gradient(135deg,#0d2c24,#1f7657);color:#ffffff">
          <div style="display:inline-block;width:42px;height:42px;line-height:42px;text-align:center;border-radius:12px;background:#e8a521;color:#17322a;font-size:18px;font-weight:900;vertical-align:middle">DI</div>
          <div style="display:inline-block;margin-left:11px;vertical-align:middle">
            <div style="font-size:24px;font-weight:850;letter-spacing:-1px">Dine<span style="color:#f2bd4c">IQ</span></div>
            <div style="margin-top:2px;color:#a9c6ba;font-size:11px;font-weight:700;letter-spacing:1px;text-transform:uppercase">Restaurant intelligence</div>
          </div>
        </div>

        <div style="padding:30px">
          <div style="color:#1f7657;font-size:11px;font-weight:800;letter-spacing:1px;text-transform:uppercase">Your account is ready</div>
          <h1 style="margin:8px 0 10px;font-size:28px;line-height:1.2;color:#17322a">Welcome to DineIQ, {safe_name}</h1>
          <p style="margin:0 0 22px;color:#6f8079;font-size:15px;line-height:1.65">
            Your DineIQ account has been created. Use the temporary details below to sign in.
          </p>

          <div style="padding:18px 20px;background:#f7faf8;border:1px solid #e4ebe7;border-radius:14px">
            <div style="margin-bottom:6px;color:#6f8079;font-size:11px;font-weight:800;text-transform:uppercase;letter-spacing:.7px">Email address</div>
            <div style="margin-bottom:17px;color:#17322a;font-size:15px;font-weight:700;word-break:break-word">{safe_email}</div>
            <div style="margin-bottom:6px;color:#6f8079;font-size:11px;font-weight:800;text-transform:uppercase;letter-spacing:.7px">Temporary password</div>
            <div style="padding:11px 13px;background:#fff5dc;border:1px solid #f2d899;border-radius:10px;color:#684a0b;font-family:Consolas,'Courier New',monospace;font-size:17px;font-weight:800;letter-spacing:.5px;word-break:break-all">{safe_password}</div>
          </div>

          <div style="margin:24px 0">
            <a href="{safe_login_url}" style="display:inline-block;padding:12px 20px;border-radius:11px;background:#1f7657;color:#ffffff;font-size:14px;font-weight:750;text-decoration:none">Sign in to DineIQ</a>
          </div>

          <div style="padding:14px 16px;background:#e7f4ee;border-left:4px solid #1f7657;border-radius:10px;color:#315f4d;font-size:13px;line-height:1.55">
            <strong>First sign-in:</strong> You will be asked to replace this temporary password before opening the dashboard.
          </div>

          <p style="margin:22px 0 0;color:#90a09a;font-size:12px;line-height:1.55">
            If you were not expecting this account, contact your DineIQ administrator and do not share these sign-in details.
          </p>
        </div>

        <div style="padding:17px 30px;background:#f8faf9;border-top:1px solid #edf2ef;color:#90a09a;font-size:11px;text-align:center">
          DineIQ Analytics &bull; Restaurant decisions backed by data
        </div>
      </div>
    </div>
    """

    try:
        resend.Emails.send({
            "from": EMAIL_FROM,
            "to": [email],
            "subject": "Your DineIQ account is ready",
            "html": body,
        })
    except Exception:
        logger.exception("Failed to send onboarding email to %s", email)
