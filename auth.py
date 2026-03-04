"""
Upstox Authentication - Get access token
Credits: Data For Traders (https://www.youtube.com/watch?v=0XQCb4aSXDQ&list=PLtq1ftYrDPQbPcHdYLsbspmmD_uZox-OR&index=5)
Original Idea: https://www.youtube.com/@DataForTraders
"""
from playwright.sync_api import Playwright, sync_playwright
from urllib.parse import parse_qs, urlparse, quote
import pyotp
import requests
import json
 
import os
import json
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration constants from environment variables
API_KEY = os.getenv('UPSTOX_API_KEY')
SECRET_KEY = os.getenv('UPSTOX_SECRET_KEY')
RURL = os.getenv('UPSTOX_REDIRECT_URI', 'https://127.0.0.1:5000/')
TOTP_KEY = os.getenv('UPSTOX_TOTP_KEY')
MOBILE_NO = os.getenv('UPSTOX_MOBILE_NO')
PIN = os.getenv('UPSTOX_PIN')

if not all([API_KEY, SECRET_KEY, TOTP_KEY, MOBILE_NO, PIN]):
    print("❌ Error: Missing required environment variables.")
    print("Please ensure UPSTOX_API_KEY, UPSTOX_SECRET_KEY, UPSTOX_TOTP_KEY, UPSTOX_MOBILE_NO, and UPSTOX_PIN are set.")
    exit(1)

rurlEncode = quote(RURL, safe="")
AUTH_URL = f'https://api-v2.upstox.com/login/authorization/dialog?response_type=code&client_id={API_KEY}&redirect_uri={rurlEncode}'


def get_access_token(code):
    """Get access token from authorization code"""
    url = 'https://api-v2.upstox.com/login/authorization/token'
    headers = {
        'accept': 'application/json',
        'Api-Version': '2.0',
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    data = {
        'code': code,
        'client_id': API_KEY,
        'client_secret': SECRET_KEY,
        'redirect_uri': RURL,
        'grant_type': 'authorization_code'
    }
    
    response = requests.post(url, headers=headers, data=data)
    return response.json()['access_token']


def run_auth(playwright: Playwright) -> str:
    """Handle authentication and get authorization code"""
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()
    
    with page.expect_request(f"*{RURL}?code*") as request:
        page.goto(AUTH_URL)
        page.locator("#mobileNum").click()
        page.locator("#mobileNum").fill(MOBILE_NO)
        page.get_by_role("button", name="Get OTP").click()
        page.locator("#otpNum").click()
        otp = pyotp.TOTP(TOTP_KEY).now()
        page.locator("#otpNum").fill(otp)
        page.get_by_role("button", name="Continue").click()
        page.get_by_label("Enter 6-digit PIN").click()
        page.get_by_label("Enter 6-digit PIN").fill(PIN)
        page.get_by_role("button", name="Continue").click()
        page.wait_for_load_state()
        url = request.value.url
        print(f"Redirect URL with code: {url}")
        parsed = urlparse(url)
        code = parse_qs(parsed.query)['code'][0]
    
    context.close()
    browser.close()
    return code


def main():
    """Main entry point - authenticate and save token"""
    print("🔐 Starting Upstox authentication...")
    
    with sync_playwright() as playwright:
        code = run_auth(playwright)
    
    access_token = get_access_token(code)
    print(f"✅ Access Token obtained")
    
    # Save to same directory as this script
    import os
    token_path = os.path.join(os.path.dirname(__file__), 'access_token.json')
    with open(token_path, 'w') as f:
        json.dump({'access_token': access_token}, f)
    
    print(f"💾 Token saved to: {token_path}")
    return access_token


if __name__ == "__main__":
    main()
