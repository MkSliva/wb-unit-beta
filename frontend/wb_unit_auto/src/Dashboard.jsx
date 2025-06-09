import { useEffect, useState } from "react";
import { LineChart, Line, CartesianGrid, XAxis, YAxis, Tooltip, Legend } from "recharts";

export default function Dashboard() {
  const [data, setData] = useState([]);
  const [groupDetails, setGroupDetails] = useState([]);
  const [selectedImt, setSelectedImt] = useState(null);
  const [startDate, setStartDate] = useState("2025-06-06");
  const [endDate, setEndDate] = useState("2025-06-09");
  const [sortConfig, setSortConfig] = useState({ key: null, direction: "asc" });
  const [showModal, setShowModal] = useState(false);
  const [chartData, setChartData] = useState([]);

  useEffect(() => {
    fetch(`http://localhost:8000/api/sales_grouped_detailed_range?start_date=${startDate}&end_date=${endDate}`)
      .then((res) => res.json())
      .then((json) => {
        setData(json.data);
        setGroupDetails([]);
        setChartData([]);
        setSelectedImt(null);
      });
  }, [startDate, endDate]);

  const fetchGroupDetails = async (imtID) => {
    try {
      const [detailsRes, chartRes] = await Promise.all([
        fetch(
          `http://localhost:8000/api/sales_by_imt?imt_id=${imtID}&start_date=${startDate}&end_date=${endDate}`
        ),
        fetch(
          `http://localhost:8000/api/sales_by_imt_daily?imt_id=${imtID}&start_date=${startDate}&end_date=${endDate}`
        )
      ]);

      const details = await detailsRes.json();
      const chart = await chartRes.json();

      if (details.data) {
        setGroupDetails(details.data);
      }

      if (chart.data) {
        setChartData(chart.data);
      }

      setSelectedImt(imtID);
      setShowModal(true);
    } catch (error) {
      console.error("Ошибка при получении деталей:", error);
    }
  };

  const sortedData = [...data].sort((a, b) => {
    if (!sortConfig.key) return 0;
    const aValue = a[sortConfig.key];
    const bValue = b[sortConfig.key];
    if (aValue < bValue) return sortConfig.direction === "asc" ? -1 : 1;
    if (aValue > bValue) return sortConfig.direction === "asc" ? 1 : -1;
    return 0;
  });

  const handleSort = (key) => {
    let direction = "asc";
    if (sortConfig.key === key && sortConfig.direction === "asc") {
      direction = "desc";
    }
    setSortConfig({ key, direction });
  };

  const totalOrders = groupDetails.reduce((acc, item) => acc + item.ordersCount, 0);
  const totalProfit = groupDetails.reduce((acc, item) => acc + item.total_profit, 0);
  const totalAd = groupDetails.reduce((acc, item) => acc + item.ad_spend, 0);
  const avgSalePrice = (groupDetails.reduce((acc, item) => acc + item.salePrice, 0) / groupDetails.length || 0).toFixed(2);
  const avgCost = (groupDetails.reduce((acc, item) => acc + item.cost_price, 0) / groupDetails.length || 0).toFixed(2);

  return (
    <div style={{ padding: "20px", fontFamily: "Arial" }}>
      <h2>📦 Группы карточек (по imtID)</h2>

      <div style={{ marginBottom: "20px" }}>
        <label>С: </label>
        <input type="date" value={startDate} onChange={(e) => setStartDate(e.target.value)} />
        <label style={{ marginLeft: "20px" }}>По: </label>
        <input type="date" value={endDate} onChange={(e) => setEndDate(e.target.value)} />
      </div>

      <table style={{ borderCollapse: "collapse", width: "100%" }}>
        <thead>
          <tr style={{ background: "#eee", cursor: "pointer" }}>
            <th onClick={() => handleSort("imtID")}>imtID</th>
            <th onClick={() => handleSort("ordersCount")}>Заказы</th>
            <th onClick={() => handleSort("ad_spend")}>Реклама</th>
            <th onClick={() => handleSort("total_profit")}>Прибыль</th>
          </tr>
        </thead>
        <tbody>
          {sortedData.map((group, index) => (
            <tr
              key={index}
              onClick={() => fetchGroupDetails(group.imtID)}
              style={{ textAlign: "center", borderBottom: "1px solid #ccc", cursor: "pointer" }}
            >
              <td>{group.imtID}</td>
              <td>{group.ordersCount}</td>
              <td>{group.ad_spend}</td>
              <td>{group.total_profit}</td>
            </tr>
          ))}
        </tbody>
      </table>

      {showModal && (
        <div style={{
          position: "fixed", top: 0, left: 0, width: "100%", height: "100%",
          background: "rgba(0,0,0,0.5)", display: "flex", justifyContent: "center", alignItems: "center"
        }}>
          <div style={{ background: "white", padding: 30, borderRadius: 8, width: "80%", maxHeight: "90%", overflowY: "auto" }}>
            <h3>📊 Статистика по связке (imtID: {selectedImt})</h3>
            <p><strong>Заказов:</strong> {totalOrders}</p>
            <p><strong>Прибыль:</strong> {totalProfit.toFixed(2)} ₽</p>
            <p><strong>Реклама:</strong> {totalAd.toFixed(2)} ₽</p>
            <p><strong>Средняя цена продажи:</strong> {avgSalePrice} ₽</p>
            <p><strong>Средняя себестоимость:</strong> {avgCost} ₽</p>

            {chartData.length > 0 && (
              <div style={{ margin: "20px 0" }}>
                <LineChart width={700} height={300} data={chartData}>
                  <CartesianGrid stroke="#ccc" />
                  <XAxis dataKey="date" />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Line type="monotone" dataKey="ordersCount" stroke="#8884d8" name="Заказы" />
                  <Line type="monotone" dataKey="ad_spend" stroke="#82ca9d" name="Реклама" />
                  <Line type="monotone" dataKey="total_profit" stroke="#ff7300" name="Прибыль" />
                </LineChart>
              </div>
            )}

            <h4>📦 Все товары в связке:</h4>
            <table style={{ borderCollapse: "collapse", width: "100%" }}>
              <thead>
                <tr style={{ background: "#f2f2f2" }}>
                  <th>vendorCode</th>
                  <th>Заказы</th>
                  <th>Реклама</th>
                  <th>Прибыль</th>
                  <th>Цена продажи</th>
                  <th>Себестоимость</th>
                </tr>
              </thead>
              <tbody>
                {groupDetails.map((item, index) => (
                  <tr key={index} style={{ textAlign: "center", borderBottom: "1px solid #ccc" }}>
                    <td>{item.vendorCode}</td>
                    <td>{item.ordersCount}</td>
                    <td>{item.ad_spend}</td>
                    <td>{item.total_profit}</td>
                    <td>{item.salePrice}</td>
                    <td>{item.cost_price}</td>
                  </tr>
                ))}
              </tbody>
            </table>

            <button
              onClick={() => {
                setShowModal(false);
                setChartData([]);
                setGroupDetails([]);
              }}
              style={{ marginTop: "20px" }}
            >
              Закрыть
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

