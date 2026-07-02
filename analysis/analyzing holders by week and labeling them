"""
a simple script for computing balances, we fetched labels of addresses via arkham and hardcoded them into script
"""
import pandas as pd
import duckdb

con = duckdb.connect()
con.execute("SET TimeZone = 'UTC';")


con.create_function("hexdec", lambda h: None if h is None else str(int(h, 16)),
                    ["VARCHAR"], "VARCHAR")

xaut_Transfers = pd.read_parquet("/raw/XAUT/Transfer.parquet")
paxg_Transfers = pd.read_parquet("/raw/PAXG/Transfer.parquet")

gold_transfers = con.sql(f"""
SELECT
'XAUT' AS token , *
, to_timestamp(block_timestamp) AS blockTime, DATE_TRUNC('WEEK', (to_timestamp(block_timestamp))) AS week_
, 6 AS decimals_
FROM xaut_Transfers
UNION ALL
SELECT
'PAXG' AS token, *
, to_timestamp(block_timestamp) AS blockTime, DATE_TRUNC('WEEK', (to_timestamp(block_timestamp))) AS week_
, 18 AS decimals_
FROM paxg_Transfers
""").df()

print(gold_transfers.iloc[0])


print(len(gold_transfers))

weeks = con.sql(f"""
SELECT DISTINCT(week_) AS week_ FROM gold_transfers
--WHERE week_ < '2024-01-01'
ORDER BY week_
""").df()

print(weeks)


def f_compute_balances_weekly(week):
    return con.sql(f"""
    WITH
    -- Net balance per address (XAUT + PAXG combined), normalized to ounces of gold.
    -- Each token's raw amount is divided by its own 10^decimals_ BEFORE summing,
    -- so the two scales (1e6 vs 1e18) are reconciled per row.
    balances AS (
        SELECT
          '{week}' AS week_
        , address
        , SUM(amount) AS balance
        FROM (
            SELECT
              CONCAT('0x', SUBSTR(topic2, 27, 40)) AS address                  -- to: credit
            , CAST(hexdec(data) AS HUGEINT) / POW(10, decimals_) AS amount
            FROM gold_transfers
            WHERE week_ <= '{week}'

            UNION ALL

            SELECT
              CONCAT('0x', SUBSTR(topic1, 27, 40)) AS address                  -- from: debit
            , -1 * CAST(hexdec(data) AS HUGEINT) / POW(10, decimals_) AS amount
            FROM gold_transfers
            WHERE week_ <= '{week}'
        )
        GROUP BY 1, 2
    )
    --select * from balances where balance < 0

    , labels_ratios AS (
        SELECT *
        -- keep an address as itself if it's in the cumulative top-50% of supply
        -- or among the top-10 holders; otherwise bucket it into 'others'
        , CASE WHEN ((ratio < 75) OR (rn < 10)) THEN address ELSE 'others' END AS addressOrOthers
        , CASE address
         WHEN '0x5754284f345afc66a98fbb0a0afe71e0f007b949' THEN 'Tether: Treasury'
         WHEN '0x7d766b06e7164be4196ee62e6036c9fcff68107d' THEN 'Paxos: Treasury'
         WHEN '0x264bd8291fae1d75db2c5f573b07faa6715997b5' THEN 'Paxos: Treasury'
         WHEN '0x2fb074fa59c9294c71246825c1c9a0c7782d41a4' THEN 'Paxos: Treasury'
         WHEN '0x742d35cc6634c0532925a3b844bc454e4438f44e' THEN 'Bitfinex'
         WHEN '0x77134cbc06cb00b66f4c7e623d5fdbf6777635ec' THEN 'Bitfinex'
         WHEN '0x6b9b774502e6afaafcac84f840ac8a0844a1abe3' THEN 'ByBit'
         WHEN '0x187c9fbf5bd0f266883c03f320260c407c7b4100' THEN 'ByBit'
         WHEN '0xf89d7b9c864f589bbf53a82105107622b35eaa40' THEN 'ByBit'
         WHEN '0x28c6c06298d514db089934071355e5743bf21d60' THEN 'Binance'
         WHEN '0x5a52e96bacdabb82fd05763e25335261b270efcb' THEN 'Binance'
         WHEN '0xf977814e90da44bfa03b6295a0616a897441acec' THEN 'Binance'
         WHEN '0x43684d03d81d3a4c70da68febdd61029d426f042' THEN 'Binance'
         WHEN '0x98adef6f2ac8572ec48965509d69a8dd5e8bba9d' THEN 'Binance'
         WHEN '0xdfd5293d8e347dfe59e90efd55b2956a1343963d' THEN 'Binance'
         WHEN '0x21a31ee1afc51d94c2efccaa2092ad1028285549' THEN 'Binance'
         WHEN '0x073f564419b625a45d8aea3bb0de4d5647113ad7' THEN 'OKX'
         WHEN '0xbb3c6d28def21b6297016622a57a0b05015e3ad2' THEN 'OKX'
         WHEN '0xb0a27099582833c0cb8c7a0565759ff145113d64' THEN 'OKX'
         WHEN '0x4a4aaa0155237881fbd5c34bfae16e985a7b068d' THEN 'OKX'
         WHEN '0x91d40e4818f4d4c57b4578d9eca6afc92ac8debe' THEN 'OKX'
         WHEN '0xadfffc33cdc9970349cbcea3d73ec343d6ed116d' THEN 'Bitget'
         WHEN '0xffa8db7b38579e6a2d14f9b347a9ace4d044cd54' THEN 'Bitget'
         WHEN '0x936298ad30a08196c2691d0bdd082f4d4dc4e9ec' THEN 'BitGo'
         WHEN '0x308672703961164e64332ba1ce19b85827502b4d' THEN 'Bitkub'
         WHEN '0xd2dd7b597fd2435b6db61ddf48544fd931e6869f' THEN 'Kraken'
         WHEN '0x7dafba1d69f6c01ae7567ffd7b046ca03b706f83' THEN 'Kraken'
         WHEN '0xa726a64b66f4bd53c1c5e355cb635f7060834c04' THEN 'Kraken'
         WHEN '0x3eba306b6fc21d8d0fb1442c8d5fd08af051bf23' THEN 'Kraken'
         WHEN '0xcc282e2004428939ee5149a9e7872f0b4d5d5ec7' THEN 'Kraken'
         WHEN '0x548054687ef6c56c6d82e8269e5fd93d8b88fcb2' THEN 'CoinEx'
         WHEN '0x2677c4c8757da1857cc7cc4071e0e0dd32ccb975' THEN 'KuCoin'
         WHEN '0x175ce6204bfda2a509c7e9c786b74407f569c9cc' THEN 'KuCoin'
         WHEN '0xaa10db8804d076601999c7cd769e02e44a99d5b2' THEN 'KuCoin'
         WHEN '0xc882b111a75c0c657fc507c04fbfcd2cc984f071' THEN 'Gate.io'
         WHEN '0x0d0707963952f2fba59dd06f2b425ace40b492fe' THEN 'Gate.io'
         WHEN '0x6bb0aa2a89f84dea0c80d8e2cbba665c46371402' THEN 'Coinbase'
         WHEN '0x5b59f901f9eff61b63f0b5960ca8b9635ac53a3f' THEN 'Coinbase'
         WHEN '0x3da1f058f69ca6fe9df3720842fd19085b633d0b' THEN 'Coinbase'
         WHEN '0xa1b4bfcaf52b6cefdfd01c40491c96a1c6b20eab' THEN 'Coinbase'
         WHEN '0x891c3eab7945d7c7f8d3beed19f650cedc45b062' THEN 'Coinbase'
         WHEN '0x2eeebc3e890c6498bb7b06625823c7e0641c32cb' THEN 'Coinbase Prime'
         WHEN '0x76ec5a0d3632b2133d9f1980903305b62678fbd3' THEN 'BtcTurk'
         WHEN '0xdf28fc45e2deb4ad87be39411369d895ed9e3702' THEN 'Upbit'
         WHEN '0xad8ae4e49e764e95c786d4f9cb0110c2a15126f0' THEN 'Gemini'
         WHEN '0xcffad3200574698b78f32232aa9d63eabd290703' THEN 'Crypto.com'
         WHEN '0xa023f08c70a23abc7edfc5b6b5e171d78dfc947e' THEN 'Crypto.com'
         WHEN '0x841ed663f2636863d40be4ee76243377dff13a34' THEN 'Robinhood'
         WHEN '0x3cc936b795a188f0e246cbb2d74c5bd190aecf18' THEN 'MEXC'
         WHEN '0x75e89d5979e4f6fba9f97c104c2f0afb3f1dcb88' THEN 'MEXC'
         WHEN '0xe3ecd65cf2ad2eba2aa2be1d0894753b2172abd1' THEN 'Bitso'
         WHEN '0x442689f3f26cbccc2e288daea986b9d67346149a' THEN 'Indodax'
         WHEN '0xf4e6deea1b4da85c2d68db8d771d37ec1148b853' THEN 'Coinhako'
         WHEN '0x1157a2076b9bb22a85cc2c162f20fab3898f4101' THEN 'FalconX'
         WHEN '0x6834e0a9105ef7fdac15ea3a497ed533d740b9fc' THEN 'Bithumb'
         WHEN '0x18e226459ccf0eec276514a4fd3b226d8961e4d1' THEN 'Binance'
         WHEN '0x22ddc64a5169a7f67cd8f444b3b48883f95fbc85' THEN 'Abraxas Capital'
         WHEN '0x53222d71015590b293c3e27e38ab1189686a7d4f' THEN 'Abraxas Capital'
         WHEN '0xed0c6079229e2d407672a117c22b62064f4a4312' THEN 'Abraxas Capital'
         WHEN '0x79ee72b4038c1da4613ac6d1c16305afe7d7b74f' THEN 'Abraxas Capital'
         WHEN '0xb99a2c4c1c4f1fc27150681b740396f6ce1cbcf5' THEN 'Abraxas Capital'
         WHEN '0xb9c2321bb7d0db468f570d10a424d1cc8efd696c' THEN 'XAUT0 LayerZero OAdapter'
         WHEN '0x95fc37a27a2f68e3a647cdc081f0a89bb47c3012' THEN 'Mantle Bridge'
         WHEN '0x8a2b6f94ff3a89a03e8c02ee92b55af90c9454a2' THEN 'AAVE'
         WHEN '0xcca852bc40e560adc3b1cc58ca5b55638ce826c9' THEN 'AAVE'
         WHEN '0xf36a47300f002c0c9f8c131962f077c3543b2fc6' THEN 'Nexo'
         WHEN '0xa99f29a2fbdcafbf057b3d8efc47cfcee670bb43' THEN 'Beam'
         WHEN '0x000000000004444c5dc75cb358380d2e3de08a90' THEN 'Uniswap'
         WHEN '0x6546055f46e866a4b9a4a13e81273e3152bae5da' THEN 'Uniswap'
         WHEN '0xc756bba710d45647715079ce50aa16aab36ded42' THEN 'Uniswap'
         WHEN '0x9c4fe5ffd9a9fc5678cfbd93aa2d4fd684b67c4c' THEN 'Uniswap'
         WHEN '0xed7ef9a9a05a48858a507c080def0405ad1eaa3e' THEN 'Uniswap'
         WHEN '0x52aa899454998be5b000ad077a46bbe360f4e497' THEN 'Fluid'
         WHEN '0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb' THEN 'Morpho'
         WHEN '0xba188b238f4e7a06c8b8b370273ca40201df717b' THEN 'Antalpha'
         WHEN '0x93008d95635ed7591bdaea980dfb26093d8cd06e' THEN 'Antalpha'
         WHEN '0x18e13eb54f68b25f1186d2134284c6066693af81' THEN 'Antalpha'
         WHEN '0x07cd80c066e13679a70e125c76f76e796c8bc748' THEN 'Antalpha'
         WHEN '0x9cbdbd7fa768ad6e9546ff57238722fa9b925329' THEN 'LMAX Digital'
         WHEN '0x51c72848c68a965f66fa7a88855f9f7784502a7f' THEN 'Wintermute'
         WHEN '0xf8191d98ae98d2f7abdfb63a9b0b812b93c873aa' THEN 'Wintermute'
         WHEN '0x785f041a4dae0c1e5edcbb081f1a2bb9684eff76' THEN 'RhinoFi'
         WHEN '0x586675a3a46b008d8408933cf42d8ff6c9cc61a1' THEN 'yoGOLD'
         ELSE 'others'
        END AS label

        FROM (
                SELECT
                  week_, address, balance
                , SUM(balance) OVER(PARTITION BY week_ ORDER BY balance DESC) AS pay
                , SUM(balance) OVER(PARTITION BY week_) AS payda
                , ((SUM(balance) OVER(PARTITION BY week_ ORDER BY balance DESC))
                   / (SUM(balance) OVER(PARTITION BY week_))) * 100 AS ratio
                , ROW_NUMBER() OVER(PARTITION BY week_ ORDER BY balance DESC) AS rn
                FROM balances
                WHERE address != '0x0000000000000000000000000000000000000000'
                AND ABS(balance) > 1e-9   -- drop float dust from fully-closed positions
            )
        order by balance DESC
    )
    SELECT
      week_
    , CASE
        WHEN label = 'others' THEN 'others'
        WHEN label IN ('AAVE','Uniswap','Morpho','Fluid','RhinoFi','yoGOLD') THEN 'DeFi-' || label
        ELSE 'Custody-' || label
      END AS label
    , SUM(balance) AS balance
    FROM labels_ratios
    GROUP BY 1, 2
""").df()


weekly_balances = []

for week in weeks['week_'] :
    print(f"computing balances for week: {week}")
    weekly_balance = f_compute_balances_weekly(week)
    weekly_balances.append(weekly_balance)


weekly_balances = pd.concat(weekly_balances, ignore_index=True)
weekly_balances = weekly_balances.sort_values(['week_', 'balance'], ascending=[True, False])

weekly_balances.to_json('weekly_balances.json', orient='records', indent=2)
weekly_balances.to_csv('weekly_balances.csv', index=False)
