"""
for visualization I use purely AI and love to playing on graphs tried many promts one after other and this is the script
that produces last product
"""


"""
weekly_balances (prefixed labels) — unique color per label via a spectrum colormap.

v6: consumes the new test3.py output where the `label` column is already
tier-prefixed ('Custody-Binance', 'DeFi-Uniswap', 'others'). Tier is read from
the prefix; no re-prefixing. Colors: nipy_spectral, DeFi in the violet/blue
band, custody in the green->red band, Others grey. Stack ordered by colormap
position (nm): lowest nm (violet, DeFi) on top, highest (red, custody) at bottom.

Outputs:
  - weekly_balances6.png
  - weekly_balances6.html
"""
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
import plotly.express as px

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "weekly_balances.json")

DEFI = {"AAVE", "Uniswap", "Morpho", "Fluid", "RhinoFi", "yoGOLD"}   # fallback for bare labels
PREFIX = {"custodian": "Custody-", "defi": "DeFi-", "other": ""}
CMAP = mpl.colormaps["nipy_spectral"]   # spectrum-style colormap (no dead zones)
DEFI_BAND = (0.22, 0.26)            # DeFi    -> violet / blue / cyan
CUST_BAND = (0.46, 0.81)            # custody -> green / yellow / orange / red

def role(label):
    if label in ("others", "Others"):              return "other"
    if label.startswith("DeFi-") or label in DEFI:  return "defi"
    return "custodian"                              # 'Custody-*' or any other holder

def display(label):
    if label in ("others", "Others"):              return "Others"
    if label.startswith(("DeFi-", "Custody-")):     return label          # already prefixed
    return PREFIX[role(label)] + label                                    # bare -> add prefix

def interleave(n):
    return list(range(0, n, 2)) + list(range(1, n, 2))

# ---- load & aggregate -------------------------------------------------------
df = pd.read_json(SRC)
df["week_"] = pd.to_datetime(df["week_"], utc=True)
df["disp"] = df["label"].map(display)
df["role"] = df["label"].map(role)
by = df.groupby(["week_", "disp", "role"], as_index=False)["balance"].sum()
by.loc[by["balance"] < 0, "balance"] = 0.0

totals = by.groupby("disp")["balance"].sum().sort_values(ascending=False)
def tier(r): return [d for d in totals.index if by.loc[by.disp == d, "role"].iloc[0] == r]
cust, defi = tier("custodian"), tier("defi")

# ---- colormap position per label (nm proxy), then colors -------------------
def spread(labels, lo, hi):
    n = len(labels)
    pos = interleave(n)
    return {labels[k]: lo + (hi - lo) * (pos[k] + 0.5) / n for k in range(n)}
cmap_pos = {}
cmap_pos.update(spread(defi, *DEFI_BAND))          # DeFi  band positions
cmap_pos.update(spread(cust, *CUST_BAND))          # custody band positions
colors = {"Others": (0.83, 0.83, 0.83)}
colors.update({lab: CMAP(p)[:3] for lab, p in cmap_pos.items()})

# stack order bottom -> top: Others base, then by colormap position (nm) DESC
# => biggest nm (red, custody) at bottom, smallest nm (violet, DeFi) at top
order = (["Others"] if "Others" in colors else []) \
        + sorted(cmap_pos, key=lambda l: cmap_pos[l], reverse=True)

# ---- last-week tier split ---------------------------------------------------
last = by["week_"].max(); lw = by[by["week_"] == last]; tot = lw["balance"].sum()
print(f"last week {last.date()} — {tot:,.0f} oz")
for r in ("custodian", "defi", "other"):
    v = lw[lw["role"] == r]["balance"].sum()
    print(f"  {r:10}: {v:11,.0f} oz ({v/tot*100:5.2f}%)")

# ---- matplotlib PNG ---------------------------------------------------------
wide = (by.pivot(index="week_", columns="disp", values="balance")
        .reindex(columns=order).fillna(0.0).sort_index())
fig, ax = plt.subplots(figsize=(17, 9))
ax.stackplot(wide.index, [wide[d].values for d in order],
             labels=order, colors=[colors[d] for d in order])
ax.set_title("Onchain gold, offchain behavior — XAUT + PAXG holders (spectrum-coded by tier)")
ax.set_xlabel("Week"); ax.set_ylabel("Balance (oz of gold)")
ax.margins(x=0); ax.grid(True, axis="y", alpha=0.3)
h, l_ = ax.get_legend_handles_labels()
ax.legend(h[::-1], l_[::-1], loc="center left", bbox_to_anchor=(1.01, 0.5),
          fontsize=7, ncol=2, title="Holder (role-tier)")
fig.tight_layout()
fig.savefig(os.path.join(HERE, "weekly_balances6.png"), dpi=130, bbox_inches="tight")
print("wrote weekly_balances6.png")

# ---- plotly HTML ------------------------------------------------------------
cmap_hex = {d: "rgb(%d,%d,%d)" % tuple(int(c * 255) for c in colors[d]) for d in order}
figp = px.area(by.sort_values("week_"), x="week_", y="balance", color="disp",
               category_orders={"disp": order}, color_discrete_map=cmap_hex,
               title="Onchain gold, offchain behavior — XAUT + PAXG holders (spectrum-coded by tier)",
               labels={"week_": "Week", "balance": "Balance (oz of gold)", "disp": "Holder (role-tier)"})
figp.update_layout(hovermode="x unified", legend_title_text="Holder (role-tier)")
figp.write_html(os.path.join(HERE, "weekly_balances6.html"))
print("wrote weekly_balances6.html")
