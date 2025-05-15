JMA_STATIONS = [
    {"id": "47412", "name": "札幌"},
    {"id": "47575", "name": "青森"},
    {"id": "47584", "name": "盛岡"},
    {"id": "47590", "name": "仙台"},
    {"id": "47582", "name": "秋田"},
    {"id": "47588", "name": "山形"},
    {"id": "47595", "name": "福島"},
    {"id": "47629", "name": "水戸"},
    {"id": "47615", "name": "宇都宮"},
    {"id": "47624", "name": "前橋"},
    {"id": "47626", "name": "熊谷"},
    {"id": "47682", "name": "千葉"},
    {"id": "47662", "name": "東京"},
    {"id": "47670", "name": "横浜"},
    {"id": "47604", "name": "新潟"},
    {"id": "47607", "name": "富山"},
    {"id": "47605", "name": "金沢"},
    {"id": "47616", "name": "福井"},
    {"id": "47638", "name": "甲府"},
    {"id": "47610", "name": "長野"},
    {"id": "47632", "name": "岐阜"},
    {"id": "47656", "name": "静岡"},
    {"id": "47636", "name": "名古屋"},
    {"id": "47651", "name": "津"},
    {"id": "47761", "name": "彦根"},
    {"id": "47759", "name": "京都"},
    {"id": "47772", "name": "大阪"},
    {"id": "47770", "name": "神戸"},
    {"id": "47780", "name": "奈良"},
    {"id": "47777", "name": "和歌山"},
    {"id": "47746", "name": "鳥取"},
    {"id": "47741", "name": "松江"},
    {"id": "47768", "name": "岡山"},
    {"id": "47765", "name": "広島"},
    {"id": "47762", "name": "下関"},
    {"id": "47895", "name": "徳島"},
    {"id": "47890", "name": "高松"},
    {"id": "47887", "name": "松山"},
    {"id": "47893", "name": "高知"},
    {"id": "47807", "name": "福岡"},
    {"id": "47813", "name": "佐賀"},
    {"id": "47817", "name": "長崎"},
    {"id": "47819", "name": "熊本"},
    {"id": "47815", "name": "大分"},
    {"id": "47830", "name": "宮崎"},
    {"id": "47827", "name": "鹿児島"},
    {"id": "47936", "name": "那覇"},
]

# JMA API Configuration
JMA_BASE_URL = "https://www.data.jma.go.jp/risk/obsdl/"
JMA_INDEX_URL = JMA_BASE_URL + "index.php"
JMA_POST_URL = JMA_BASE_URL + "show/table"  # For CSV download

# Retry Configuration
MAX_RETRIES = 3
INITIAL_RETRY_DELAY_SECONDS = 5
DEFAULT_REQUEST_RATE_SECONDS = 1.2 # Default seconds to wait between JMA API requests

# Data Element Configuration
# Default elements: Temperature, Precipitation, Sunshine Duration, Wind, Snow Depth, Solar Radiation
DEFAULT_ELEMENT_CODES = ["201", "401", "301", "610", "703", "503"]

# Wind Direction Mapping
WIND_DIRECTION_TO_DEGREES = {
    "北": 0.0,
    "北北東": 22.5,
    "北東": 45.0,
    "東北東": 67.5,
    "東": 90.0,
    "東南東": 112.5,
    "南東": 135.0,
    "南南東": 157.5,
    "南": 180.0,
    "南南西": 202.5,
    "南西": 225.0,
    "西南西": 247.5,
    "西": 270.0,
    "西北西": 292.5,
    "北西": 315.0,
    "北北西": 337.5,
    "静穏": None,  # Calm
}
