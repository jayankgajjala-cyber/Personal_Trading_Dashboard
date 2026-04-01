# Quantedge — Portfolio Command Center

> High-density, dark-mode portfolio management dashboard with Zerodha CSV import, OTP-based auth, and AI Signal placeholder.

---

## Stack

| Layer | Technology |
|---|---|
| Frontend | Next.js 14 (App Router) + Tailwind + Shadcn/UI |
| Backend | FastAPI (Python 3.11) |
| Database | Supabase (PostgreSQL) |
| Auth | Username/Password → Resend OTP → JWT |
| Deploy | Vercel (frontend) + Railway (backend) |
| Container | Docker + docker-compose |

---

## Quick Start (Local)

### 1. Clone & configure

```bash
git clone <your-repo>
cd quantedge
cp .env.example .env
# Fill in all values in .env
```

### 2. Run with Docker Compose

```bash
docker-compose up --build
```

- Frontend: http://localhost:3000  
- Backend API: http://localhost:8000  
- API Docs: http://localhost:8000/docs

### 3. Run the Supabase migration

Open **Supabase Dashboard → SQL Editor** and paste + run `supabase_migration.sql`.

---

## Manual Dev Setup

### Backend

```bash
cd backend
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp ../.env.example .env   # fill values
uvicorn app.main:app --reload
```

### Frontend

```bash
cd frontend
npm install
cp ../.env.example .env.local
# Set NEXT_PUBLIC_API_URL=http://localhost:8000
npm run dev
```

---

## Auth Flow

```
1. POST /auth/login       { username, password }
   → validates credentials
   → generates 6-digit OTP
   → emails OTP via Resend API

2. POST /auth/verify-otp  { username, otp }
   → verifies OTP (5-min TTL)
   → issues JWT (60-min expiry)

3. All /holdings/* routes require Bearer JWT
```

---

## API Endpoints

### Auth
| Method | Path | Description |
|---|---|---|
| POST | `/auth/login` | Validate credentials, send OTP |
| POST | `/auth/verify-otp` | Verify OTP, get JWT |
| GET | `/auth/me` | Get current user |

### Holdings
| Method | Path | Description |
|---|---|---|
| GET | `/holdings` | List all holdings |
| POST | `/holdings` | Add new holding |
| PATCH | `/holdings/{symbol}` | Add shares (weighted avg) |
| DELETE | `/holdings/{symbol}` | Remove holding |
| POST | `/holdings/upload-csv` | Import Zerodha CSV |

---

## Weighted Average Formula

When adding shares to an existing position:

```
New_Avg = ((Current_Qty × Current_Avg) + (New_Qty × Buy_Price))
          / (Current_Qty + New_Qty)
```

---

## CSV Import Format (Zerodha)

Export your holdings from Zerodha Kite and upload. Required columns:

| Column | Maps To |
|---|---|
| `Instrument` | `symbol` |
| `Qty.` | `quantity` |
| `Avg. cost` | `average_buy_price` |

If a symbol already exists, weighted average is applied automatically.

---

## Production Deployment

### Frontend → Vercel

```bash
cd frontend
vercel --prod
# Set NEXT_PUBLIC_API_URL to your Railway backend URL in Vercel env vars
```

### Backend → Railway

1. Connect your GitHub repo
2. Set root directory to `backend/`
3. Add all env vars from `.env.example`
4. Railway auto-detects the `Dockerfile`

---

## Environment Variables

See `.env.example` for all required variables with descriptions.

---

## Roadmap

- [ ] Live LTP feed via WebSocket (NSE/BSE data provider)
- [ ] AI Signal column (momentum / sentiment analysis)
- [ ] P&L chart (daily / weekly / all-time)
- [ ] Sector allocation pie chart
- [ ] Tax harvesting suggestions
