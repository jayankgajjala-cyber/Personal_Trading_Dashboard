import axios from "axios";
import Cookies from "js-cookie";

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

export const api = axios.create({
  baseURL: API_URL,
  headers: { "Content-Type": "application/json" },
});

api.interceptors.request.use((config) => {
  const token = Cookies.get("qe_token");
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

api.interceptors.response.use(
  (res) => res,
  (err) => {
    if (err.response?.status === 401 && typeof window !== "undefined") {
      Cookies.remove("qe_token");
      window.location.href = "/auth/login";
    }
    return Promise.reject(err);
  }
);

export const authApi = {
  login: (username: string, password: string) =>
    api.post("/auth/login", { username, password }),
  resendOtp: (username: string, password: string) =>
    api.post("/auth/resend-otp", { username, password }),
  verifyOtp: (username: string, otp: string) =>
    api.post<{ access_token: string }>("/auth/verify-otp", { username, otp }),
  me: () => api.get("/auth/me"),
};

export const holdingsApi = {
  list: () => api.get<Holding[]>("/holdings"),
  create: (data: CreateHoldingPayload) => api.post<Holding>("/holdings", data),
  addShares: (symbol: string, additional_quantity: number, buy_price: number) =>
    api.patch<Holding>(`/holdings/${symbol}`, { additional_quantity, buy_price }),
  sellShares: (symbol: string, sell_quantity: number, sell_price: number) =>
    api.post(`/holdings/${symbol}/sell`, { sell_quantity, sell_price }),
  delete: (symbol: string) => api.delete(`/holdings/${symbol}`),
  uploadCsv: (file: File) => {
    const form = new FormData();
    form.append("file", file);
    return api.post("/holdings/upload-csv", form, {
      headers: { "Content-Type": "multipart/form-data" },
    });
  },
};

export interface Holding {
  id: string;
  symbol: string;
  stock_name: string;
  quantity: number;
  average_buy_price: number;
  invested_amount: number;
  exchange: string;
  ltp: number | null;
  current_value: number | null;
  pnl: number | null;
  pnl_percent: number | null;
  signal: string | null;
  created_at: string;
  updated_at: string;
}

export interface CreateHoldingPayload {
  symbol: string;
  stock_name: string;
  quantity: number;
  average_buy_price: number;
  exchange: string;
}
