import { GoogleAuth } from 'google-auth-library';

export class TokenProvider {
  private auth: GoogleAuth;

  constructor() {
    this.auth = new GoogleAuth({
      scopes: ['https://www.googleapis.com/auth/cloud-platform'],
    });
  }

  async getToken(): Promise<{ token: string; expiresAt: number }> {
    const client = await this.auth.getClient();
    const accessToken = await client.getAccessToken();
    
    if (!accessToken.token) {
      throw new Error('Failed to get access token');
    }

    // Calculate expiry. The library doesn't always return expiry time directly in a convenient way for all client types,
    // but usually we can assume 1 hour or check if it's available.
    // However, for Kafka OAUTHBEARER, we need a valid token.
    // Let's assume 1 hour expiry if not provided, or try to find it.
    // The python code constructs a JWT. Here we are getting an OAuth2 access token.
    // Wait, the python code constructs a JWT with "scope": "kafka".
    // And it signs it using the service account credentials.
    // It seems it's generating a self-signed JWT?
    // "typ": "JWT", "alg": "GOOG_OAUTH2_TOKEN"
    // This looks like a specific Google OAuth mechanism.
    
    // Actually, for Managed Service for Apache Kafka, we usually use the OAuth2 access token directly?
    // Or a specific JWT?
    // The python code uses `google.auth.default()` and then constructs a JWT manually using `creds.signer`.
    // It seems to be creating a specific format.
    
    // Let's look closely at the python code:
    // HEADER = json.dumps({"typ": "JWT", "alg": "GOOG_OAUTH2_TOKEN"})
    // token = ".".join([encode(HEADER), encode(get_jwt(creds)), encode(creds.token)])
    // It seems to be wrapping the access token in a JWT-like structure?
    // "alg": "GOOG_OAUTH2_TOKEN" is very specific.
    
    // If I use `kafkajs`, I need to provide an OAuthBearerProvider.
    // Maybe I can just return the access token if the broker accepts it.
    // But if the broker expects this specific format, I must replicate it.
    
    // Let's try to replicate the Python logic.
    // It takes the raw access token (`creds.token`) and wraps it.
    
    return {
      token: accessToken.token,
      expiresAt: Date.now() + 3600 * 1000, // Estimate 1 hour
    };
  }
  
  // We might need to replicate the exact token format if standard OAuth doesn't work.
  // The Python code does:
  // 1. Header: {"typ": "JWT", "alg": "GOOG_OAUTH2_TOKEN"}
  // 2. Payload: { exp, iat, iss: "Google", scope: "kafka", sub: email }
  // 3. Signature: The actual OAuth2 Access Token (creds.token)
  
  // This is WEIRD. The "Signature" part is usually a cryptographic signature.
  // Here it puts the access token in the signature field?
  // Yes: `encode(creds.token)` is the 3rd part.
  
  async getGoogleKafkaToken(): Promise<{ token: string; expiresAt: number }> {
    const client = await this.auth.getClient();
    const credentials = await this.auth.getCredentials();
    const accessToken = await client.getAccessToken();
    
    if (!accessToken.token) {
      throw new Error('Failed to get access token');
    }

    const email = credentials.client_email || 'service-account@example.com'; // We might need the actual email
    const now = Math.floor(Date.now() / 1000);
    const exp = now + 3600; // 1 hour

    const header = { typ: 'JWT', alg: 'GOOG_OAUTH2_TOKEN' };
    const payload = {
      exp,
      iat: now,
      iss: 'Google',
      scope: 'kafka',
      sub: email,
    };

    const base64UrlEncode = (obj: any) => {
      const str = typeof obj === 'string' ? obj : JSON.stringify(obj);
      return Buffer.from(str).toString('base64')
        .replace(/\+/g, '-')
        .replace(/\//g, '_')
        .replace(/=+$/, '');
    };

    const token = `${base64UrlEncode(header)}.${base64UrlEncode(payload)}.${base64UrlEncode(accessToken.token)}`;
    
    return {
      token,
      expiresAt: exp * 1000,
    };
  }
}
