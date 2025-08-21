# scm/yahoo_finance.py
import itertools, pathlib, time, json
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Any, Optional
import yfinance as yf
import pandas as pd
import time

OUTDIR = pathlib.Path("data"); OUTDIR.mkdir(parents=True, exist_ok=True)

# Keep this narrow and consistent with your FMP set where possible
SYMBOL_MAP: Dict[str, str] = {
    "AIXTRON": "AIXA.DE",
    "Applied Materials": "AMAT",
    "AJ": "2802.T",
    "ASML": "ASML",
    "Naura": "002371.SZ",
    "TEL (Tokyo Electron)": "8035.T",
    "KLA": "KLAC",
    "CAMECA (AMTEK)": "AME",
    "OSI Systems": "OSIS",
    "Genes Tech": "8257.HK",
    "Scientech": "3583.TW",
}


def _to_dec_safe(x):
    try:
        if x in (None, "", "None"): return None
        return Decimal(str(x))
    except (InvalidOperation, ValueError):
        return None

def _fmt(x, nd=4): return None if x is None else float(round(x, nd))
def _pct(x): return None if x is None else x * Decimal(100)
def _div(a, b): return None if (a is None or b in (None, 0)) else a / b

# Yahoo field aliases (varies by ticker)
INC_FIELDS = {
    "revenue": ["Total Revenue", "TotalRevenue", "Revenue"],
    "grossProfit": ["Gross Profit", "GrossProfit"],
    "netIncome": ["Net Income", "NetIncome"],
    "rd": ["Research Development", "Research And Development", "R&D", "ResearchDevelopment"]
}

BAL_FIELDS = {
    "cash": ["Cash And Cash Equivalents","CashAndCashEquivalents","Cash"],
    "shortDebt": ["Short/Current Long Term Debt","Short Term Debt","Short Long Term Debt","CurrentDebtAndCapitalLeaseObligation","Current Debt And Capital Lease Obligation"],
    "longDebt": ["Long Term Debt","LongTermDebt","LongTermDebtAndCapitalLeaseObligation", "Long Term Debt And Capital Lease Obligation"],
    "totalLiab": ["Total Liab","Total Liabilities","TotalLiabilitiesNetMinorityInterest", "Total Liabilities Net Minority Interest"],
    "equity": ["StockholdersEquity","Total Stockholder Equity","Total Stockholders Equity","Total Shareholder Equity",
               "Total Equity Gross Minority Interest","Total Equity"],
    "currAssets": ["Total Current Assets","CurrentAssets", "Current Assets"],
    "currLiab": ["CurrentLiabilities","Total Current Liabilities","Current Liabilities"],


}

CF_FIELDS = {
    "opCF": ["Operating Cash Flow","OperatingCashFlow","Total Cash From Operating Activities","Net Cash Provided By Operating Activities"],
    "capex": ["CapitalExpenditure","Capital Expenditures","Capital Expenditure"],
}



def get_first(df: pd.DataFrame, name_list: List[str]) -> Optional[pd.Series]:
    for n in name_list:
        if n in df.index:
            return df.loc[n]
    return None

def build_rows_from_dfs(company: str, symbol: str,
                         inc_df: pd.DataFrame, bal_df: pd.DataFrame, cf_df: pd.DataFrame,
                         freq_label: str) -> List[Dict[str, Any]]:

    # Extract series for each metric (series indexed by columns=dates)
    rev_s = get_first(inc_df, INC_FIELDS["revenue"])
    gp_s  = get_first(inc_df, INC_FIELDS["grossProfit"])
    ni_s  = get_first(inc_df, INC_FIELDS["netIncome"])
    rd_s  = get_first(inc_df, INC_FIELDS["rd"])

    cash_s   = get_first(bal_df, BAL_FIELDS["cash"])
    sdebt_s  = get_first(bal_df, BAL_FIELDS["shortDebt"])
    ldebt_s  = get_first(bal_df, BAL_FIELDS["longDebt"])
    tliab_s  = get_first(bal_df, BAL_FIELDS["totalLiab"])
    equity_s = get_first(bal_df, BAL_FIELDS["equity"])
    ca_s     = get_first(bal_df, BAL_FIELDS["currAssets"])
    cl_s     = get_first(bal_df, BAL_FIELDS["currLiab"])

    ocf_s   = get_first(cf_df, CF_FIELDS["opCF"])
    capex_s = get_first(cf_df, CF_FIELDS["capex"])

    # Gather all date columns present & order them
    def cols(s: Optional[pd.Series]) -> List[pd.Timestamp]:
        return list(s.index) if s is not None else []
    dates = sorted(set(itertools.chain(
        cols(rev_s), cols(gp_s), cols(ni_s), cols(rd_s),
        cols(cash_s), cols(sdebt_s), cols(ldebt_s), cols(tliab_s),
        cols(equity_s), cols(ca_s), cols(cl_s),
        cols(ocf_s), cols(capex_s)
    )), reverse=True)

    out: List[Dict[str, Any]] = []
    for d in dates:
        # Values can be NaN → drop to None early
        def val(s: Optional[pd.Series]):
            if s is None: return None
            v = s.get(d, None)
            if pd.isna(v): return None
            return _to_dec_safe(v)

        totalRevenue = val(rev_s)
        grossProfit  = val(gp_s)
        netIncome    = val(ni_s)
        rd_expense   = val(rd_s)

        cash       = val(cash_s)
        shortDebt  = val(sdebt_s)
        longDebt   = val(ldebt_s)
        totalLiab  = val(tliab_s)
        equity     = val(equity_s)
        currAssets = val(ca_s)
        currLiab   = val(cl_s)

        opCF  = val(ocf_s)
        capex = val(capex_s)
        fcf   = (opCF - capex) if None not in (opCF, capex) else None

        totalDebt = None
        if shortDebt is not None or longDebt is not None:
            totalDebt = (shortDebt or Decimal(0)) + (longDebt or Decimal(0))

        ds = str(d.date()) if hasattr(d, "date") else str(d)  # ISO date
        row = {
            "company": company,
            "symbol": symbol,
            "freq": freq_label,
            "fiscalDate": ds,
            "metrics": {
                "fiscalDate": ds,
                "totalRevenue": _fmt(totalRevenue, 2),
                "grossProfit": _fmt(grossProfit, 2),
                "netIncome": _fmt(netIncome, 2),
                "grossMargin_pct": _fmt(_pct(_div(grossProfit, totalRevenue)), 2),
                "netMargin_pct": _fmt(_pct(_div(netIncome, totalRevenue)), 2),
                "operatingCashflow": _fmt(opCF, 2),
                "capitalExpenditures": _fmt(capex, 2),
                "freeCashFlow": _fmt(fcf, 2),
                "cashAndCashEquivalents": _fmt(cash, 2),
                "shortTermDebt": _fmt(shortDebt, 2),
                "longTermDebt": _fmt(longDebt, 2),
                "totalDebt": _fmt(totalDebt, 2) if totalDebt is not None else None,
                "totalLiabilities": _fmt(totalLiab, 2),
                "totalShareholderEquity": _fmt(equity, 2),
                "totalCurrentAssets": _fmt(currAssets, 2),
                "totalCurrentLiabilities": _fmt(currLiab, 2),
                "debtToEquity": _fmt(_div(totalLiab, equity), 4),
                "currentRatio": _fmt(_div(currAssets, currLiab), 4),
                "cashRatio": _fmt(_div(cash, currLiab), 4),
                "researchAndDevelopment": _fmt(rd_expense, 2),
                "rdIntensity_pct": _fmt(_pct(_div(rd_expense, totalRevenue)), 2),
                "capexToRevenue_pct": _fmt(_pct(_div(capex, totalRevenue)), 2),
            },
            "summary": f"{company} {freq_label} {ds}: Rev={_fmt(totalRevenue,2)}, NI={_fmt(netIncome,2)}, GM%={_fmt(_pct(_div(grossProfit,totalRevenue)),2)}, NM%={_fmt(_pct(_div(netIncome,totalRevenue)),2)}",
            "source": "YahooFinance"
        }
        # quick flags
        m = row["metrics"]
        flags = []
        if (m.get("currentRatio") is not None) and (m["currentRatio"] < 1): flags.append("current_ratio_lt_1") #potential liquidity issues for company
        if (m.get("freeCashFlow") is not None) and (m["freeCashFlow"] < 0): flags.append("negative_fcf") #no cash left over after its operating expenses & capital expenditures 
        if (m.get("debtToEquity") is not None) and (m["debtToEquity"] > 2): flags.append("high_debt_to_equity") #heavily reliabnt on borrowed funds to finance operations
        m["distressFlags"] = flags
        out.append(row)

    # QoQ / YoY on sorted list
    by_date = {r["fiscalDate"]: r for r in out}
    for i, row in enumerate(out):
        m = row["metrics"]
        if i + 1 < len(out):
            prev_m = out[i + 1]["metrics"]
            for k in ("totalRevenue", "netIncome"):
                cv, pv = m.get(k), prev_m.get(k)
                if all(isinstance(v, (int, float)) for v in (cv, pv)) and pv:
                    m[f"{k}_QoQ_growth_pct"] = round((cv - pv) / pv * 100, 2)
        try:
            mmdd = row["fiscalDate"][5:]
            prev_y = by_date.get(str(int(row["fiscalDate"][:4]) - 1) + "-" + mmdd)
            if prev_y:
                prev_m = prev_y["metrics"]
                for k in ("totalRevenue", "netIncome"):
                    cv, pv = m.get(k), prev_m.get(k)
                    if all(isinstance(v, (int, float)) for v in (cv, pv)) and pv:
                        m[f"{k}_YoY_growth_pct"] = round((cv - pv) / pv * 100, 2)
        except: 
            pass

    return out

def fetch_yahoo_financials(company: str, symbol: str) -> List[Dict[str, Any]]:
    if not symbol:
        print(f"[warn] No symbol for {company}")
        return []
    t = yf.Ticker(symbol)

    # Annual data
    inc_a = t.financials
    bal_a = t.balance_sheet
    cf_a  = t.cashflow

    # Quarterly data
    inc_q = t.quarterly_financials
    bal_q = t.quarterly_balance_sheet
    cf_q  = t.quarterly_cashflow

    #yfinance dataframes : row = account, col=period(Timestamp)
    records: List[Dict[str,Any]] = []
    records += build_rows_from_dfs(company, symbol, inc_q, bal_q, cf_q, "Quarter")
    records += build_rows_from_dfs(company, symbol, inc_a, bal_a, cf_a, "Annual")
    return records

def write_outputs(records: List[Dict[str, Any]]):
    
    jsonl_path = OUTDIR / "yahoo_finance.jsonl"
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    
    flat_rows = []
    for rec in records:
        m = rec["metrics"]
        flat_rows.append({
            "symbol": rec["symbol"],
            "frequency": rec["freq"],
            "fiscalDate": rec["fiscalDate"],
            "revenue": m.get("totalRevenue"),
            "grossProfit": m.get("grossProfit"),
            "netIncome": m.get("netIncome"),
            "grossMargin_pct": m.get("grossMargin_pct"),
            "netMargin_pct": m.get("netMargin_pct"),
            "revenue_QoQ_growth_pct": m.get("totalRevenue_QoQ_growth_pct"),
            "revenue_YoY_growth_pct": m.get("totalRevenue_YoY_growth_pct"),
            "netIncome_QoQ_growth_pct": m.get("netIncome_QoQ_growth_pct"),
            "netIncome_YoY_growth_pct": m.get("netIncome_YoY_growth_pct"),
            "operatingCashflow": m.get("operatingCashflow"),
            "freeCashFlow": m.get("freeCashFlow"),
            "debtToEquity": m.get("debtToEquity"),
            "currentRatio": m.get("currentRatio"),
            "cashRatio": m.get("cashRatio"),
            "researchAndDevelopment": m.get("researchAndDevelopment"),
            "rdIntensity_pct": m.get("rdIntensity_pct"),
            "capexToRevenue_pct": m.get("capexToRevenue_pct"),
            "source": "YahooFinance"
        })
    df = pd.DataFrame(flat_rows)
    if not df.empty:
        df.sort_values(["symbol", "frequency", "fiscalDate"], ascending=[True, True, False], inplace=True)
    csv_path = OUTDIR / "yahoo_finance.csv"
    df.to_csv(csv_path, index=False)

    print("Wrote:")
    print("-", jsonl_path)
    print("-", csv_path)

def main():
    all_records = []
    for company, symbol in SYMBOL_MAP.items():
        print(f"[Yahoo] {company} → {symbol}")
        try:
            recs = fetch_yahoo_financials(company, symbol)
            if not recs:
                print(f"[info] No financials returned for {company} ({symbol})")
            all_records.extend(recs)
        except Exception as e:
            print(f"[warn] {company} ({symbol}) failed: {type(e).__name__}: {e}")
        time.sleep(0.3)  # be gentle
    write_outputs(all_records)

if __name__ == "__main__":
    main()
