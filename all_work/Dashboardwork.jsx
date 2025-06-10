import React, { useState, useEffect, useCallback, useMemo } from "react"; // Добавлен useMemo

const Dashboard = () => {
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [totalProfit, setTotalProfit] = useState(0);
  const [groupedSales, setGroupedSales] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const [isModalOpen, setIsModalOpen] = useState(false);
  const [selectedImt, setSelectedImt] = useState(null);
  const [groupDetails, setGroupDetails] = useState([]);
  const [dailySales, setDailySales] = useState([]);
  const [editData, setEditData] = useState({});

  // *** СОСТОЯНИЕ ДЛЯ СОРТИРОВКИ ***
  const [sortConfig, setSortConfig] = useState({ key: null, direction: 'ascending' });

  // *** УДАЛЕННЫЕ ФИЛЬТРЫ (ЕСЛИ НЕ НУЖНЫ, ИНАЧЕ ОСТАВЬТЕ ПРЕДЫДУЩИЙ ВАРИАНТ) ***
  // Если вы хотите только сортировку, а не фильтры по каждому столбцу,
  // то можете удалить state filters и handleFilterChange,
  // а также логику формирования queryParams в fetchData.
  // Если вам нужны и фильтры, и сортировка, то оставьте filters state и его логику.
  // const [filters, setFilters] = useState({ ... });
  // const handleFilterChange = (e) => { ... };

  const [newBatchData, setNewBatchData] = useState({
    vendor_code: "",
    purchase_price: "",
    quantity_bought: "",
    start_date: new Date().toISOString().split("T")[0],
  });

  useEffect(() => {
    const today = new Date();
    const thirtyDaysAgo = new Date(today);
    thirtyDaysAgo.setDate(today.getDate() - 30);

    setEndDate(today.toISOString().split("T")[0]);
    setStartDate(thirtyDaysAgo.toISOString().split("T")[0]);
  }, []);

  const fetchData = useCallback(async () => {
    if (!startDate || !endDate) return;

    setLoading(true);
    setError(null);

    const queryParams = new URLSearchParams({
      start_date: startDate,
      end_date: endDate,
    });

    // Если вы решили оставить фильтры, то здесь добавьте их логику:
    // if (filters.imtid) queryParams.append("imtid_filter", filters.imtid);
    // ... и так далее для всех фильтров

    try {
      const response = await fetch(
        `http://localhost:8000/api/sales_grouped_detailed_range?${queryParams.toString()}`
      );
      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(
          `HTTP error! status: ${response.status}, message: ${errorText}`
        );
      }
      const data = await response.json();
      setGroupedSales(data.data);
      setTotalProfit(data.total_profit);
    } catch (err) {
      console.error("Error fetching grouped sales:", err);
      setError("Не удалось загрузить данные о продажах. " + err.message);
    } finally {
      setLoading(false);
    }
  }, [startDate, endDate]); // Удалены filters из зависимостей, если вы убрали фильтрацию

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  // *** ФУНКЦИЯ ДЛЯ ОБРАБОТКИ СОРТИРОВКИ ***
  const requestSort = (key) => {
    let direction = 'ascending';
    // Если уже сортируем по этому ключу и направление - "ascending", меняем на "descending"
    if (sortConfig.key === key && sortConfig.direction === 'ascending') {
      direction = 'descending';
    }
    setSortConfig({ key, direction });
  };

  // *** МЕМОИЗИРОВАННЫЕ ОТСОРТИРОВАННЫЕ ДАННЫЕ ***
  const sortedGroupedSales = useMemo(() => {
    let sortableItems = [...groupedSales]; // Создаем копию массива
    if (sortConfig.key !== null) {
      sortableItems.sort((a, b) => {
        const valA = a[sortConfig.key];
        const valB = b[sortConfig.key];

        // Обработка NaN/null значений: перемещаем их в конец при сортировке
        // (или в начало, в зависимости от желаемого поведения)
        if (valA === null || isNaN(valA) || typeof valA === 'undefined') return sortConfig.direction === 'ascending' ? 1 : -1;
        if (valB === null || isNaN(valB) || typeof valB === 'undefined') return sortConfig.direction === 'ascending' ? -1 : 1;

        if (valA < valB) {
          return sortConfig.direction === 'ascending' ? -1 : 1;
        }
        if (valA > valB) {
          return sortConfig.direction === 'ascending' ? 1 : -1;
        }
        return 0;
      });
    }
    return sortableItems;
  }, [groupedSales, sortConfig]);

  const fetchGroupDetails = useCallback(
    async (imtId) => {
      setLoading(true);
      setError(null);
      try {
        const detailsResponse = await fetch(
          `http://localhost:8000/api/sales_by_imt?imt_id=${imtId}&start_date=${startDate}&end_date=${endDate}`
        );
        if (!detailsResponse.ok) {
          throw new Error(`HTTP error! status: ${detailsResponse.status}`);
        }
        const detailsData = await detailsResponse.json();
        setGroupDetails(detailsData.data);

        const dailyResponse = await fetch(
          `http://localhost:8000/api/sales_by_imt_daily?imt_id=${imtId}&start_date=${startDate}&end_date=${endDate}`
        );
        if (!dailyResponse.ok) {
          throw new Error(`HTTP error! status: ${dailyResponse.status}`);
        }
        const dailyData = await dailyResponse.json();
        setDailySales(dailyData.data);

        if (detailsData.data.length > 0) {
          const firstVendorData = detailsData.data[0];
          setEditData({
            vendorcode: firstVendorData.vendorcode || "",
            purchase_price: firstVendorData.purchase_price || 0,
            delivery_to_warehouse: firstVendorData.delivery_to_warehouse || 0,
            wb_commission_rub: firstVendorData.wb_commission_rub || 0,
            wb_logistics: firstVendorData.wb_logistics || 0,
            tax_rub: firstVendorData.tax_rub || 0,
            packaging: firstVendorData.packaging || 0,
            fuel: firstVendorData.fuel || 0,
            gift: firstVendorData.gift || 0,
            defect_percent: firstVendorData.defect_percent || 0,
            start_date: startDate,
          });
        }
      } catch (err) {
        console.error("Error fetching group details:", err);
        setError("Не удалось загрузить детали группы. " + err.message);
      } finally {
        setLoading(false);
      }
    },
    [startDate, endDate]
  );

  const openModal = (imtId) => {
    setSelectedImt(imtId);
    fetchGroupDetails(imtId);
    setIsModalOpen(true);
  };

  const closeModal = () => {
    setIsModalOpen(false);
    setSelectedImt(null);
    setGroupDetails([]);
    setDailySales([]);
    setEditData({});
    setError(null);
  };

  const handleEditChange = (e) => {
    const { name, value } = e.target;
    setEditData((prev) => ({
      ...prev,
      [name]: ["vendorcode", "start_date"].includes(name)
        ? value
        : parseFloat(value) || 0,
    }));
  };

  const handleNewBatchChange = (e) => {
    const { name, value } = e.target;
    setNewBatchData((prev) => ({ ...prev, [name]: value }));
  };

  const submitCostUpdate = async () => {
    const dataToSend = { ...editData, start_date: startDate };
    try {
      const response = await fetch("http://localhost:8000/api/update_costs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(dataToSend),
      });
      if (response.ok) {
        alert("Данные себестоимости обновлены!");
        fetchData();
        if (selectedImt) {
          fetchGroupDetails(selectedImt);
        }
      } else {
        const errorData = await response.json();
        console.error("Ошибка при обновлении себестоимости:", errorData);
        alert(
          `Ошибка при обновлении себестоимости: ${
            JSON.stringify(errorData.detail || errorData.message)
          }`
        );
      }
    } catch (error) {
      console.error("Сетевая ошибка при обновлении себестоимости:", error);
      alert("Произошла сетевая ошибка. Проверьте консоль.");
    }
  };

  const submitNewBatch = async () => {
    if (
      !newBatchData.vendor_code ||
      !newBatchData.purchase_price ||
      !newBatchData.quantity_bought ||
      !newBatchData.start_date
    ) {
      alert("Пожалуйста, заполните все поля для новой партии.");
      return;
    }

    const dataToSend = {
      ...newBatchData,
      purchase_price: parseFloat(newBatchData.purchase_price),
      quantity_bought: parseInt(newBatchData.quantity_bought, 10),
    };

    try {
      const response = await fetch("http://localhost:8000/api/purchase_batches", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(dataToSend),
      });
      if (response.ok) {
        console.log("Новая партия успешно добавлена!");
        alert("Новая закупочная партия добавлена!");
        setNewBatchData({
          vendor_code: "",
          purchase_price: "",
          quantity_bought: "",
          start_date: new Date().toISOString().split("T")[0],
        });
        fetchData();
        if (selectedImt) {
          fetchGroupDetails(selectedImt);
        }
      } else {
        const errorData = await response.json();
        console.error("Ошибка при добавлении новой партии:", errorData);
        alert(
          `Ошибка при добавлении партии: ${
            JSON.stringify(errorData.detail || errorData.message)
          }`
        );
      }
    } catch (error) {
      console.error("Сетевая ошибка при добавлении новой партии:", error);
      alert("Произошла сетевая ошибка. Проверьте консоль.");
    }
  };

  const checkBatches = async () => {
    try {
      const response = await fetch("http://localhost:8000/api/check_purchase_batches");
      if (response.ok) {
        const result = await response.json();
        alert(
          `Проверка партий завершена. Деактивировано: ${
            result.deactivated_batches.join(", ") || "ничего"
          }. Проверьте Telegram.`
        );
        console.log("Check batches result:", result);
        fetchData();
        if (selectedImt) {
          fetchGroupDetails(selectedImt);
        }
      } else {
        const errorData = await response.json();
        console.error("Ошибка при проверке партий:", errorData);
        alert(
          `Ошибка при проверке партий: ${
            JSON.stringify(errorData.detail || errorData.message)
          }`
        );
      }
    } catch (error) {
      console.error("Сетевая ошибка при проверке партий:", error);
      alert("Произошла сетевая ошибка. Проверьте консоль.");
    }
  };

  // Вспомогательная функция для получения индикатора сортировки
  const getSortIndicator = (key) => {
    if (sortConfig.key === key) {
      return sortConfig.direction === 'ascending' ? ' ▲' : ' ▼';
    }
    return '';
  };

  return (
    <div className="container mx-auto p-4">
      <h1 className="text-2xl font-bold mb-4">WB Аналитика Продаж</h1>

      <div className="flex space-x-4 mb-4">
        <div>
          <label
            htmlFor="startDate"
            className="block text-sm font-medium text-gray-700"
          >
            Начальная дата:
          </label>
          <input
            type="date"
            id="startDate"
            value={startDate}
            onChange={(e) => setStartDate(e.target.value)}
            className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-300 focus:ring focus:ring-indigo-200 focus:ring-opacity-50"
          />
        </div>
        <div>
          <label
            htmlFor="endDate"
            className="block text-sm font-medium text-gray-700"
          >
            Конечная дата:
          </label>
          <input
            type="date"
            id="endDate"
            value={endDate}
            onChange={(e) => setEndDate(e.target.value)}
            className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-300 focus:ring focus:ring-indigo-200 focus:ring-opacity-50"
          />
        </div>
        <button
          onClick={fetchData}
          className="mt-6 px-4 py-2 bg-blue-500 text-white rounded-md hover:bg-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-opacity-50"
          disabled={loading}
        >
          {loading ? "Загрузка..." : "Применить фильтр"}
        </button>
      </div>

      {error && <div className="text-red-600 mb-4">{error}</div>}

      <div className="mb-4">
        <h2 className="text-xl font-semibold">
          Общая прибыль:{" "}
          <span
            className={`${totalProfit >= 0 ? "text-green-600" : "text-red-600"}`}
          >
            {totalProfit.toFixed(2)} руб.
          </span>
        </h2>
      </div>

      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200 shadow-md rounded-lg">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                IMT ID
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Артикулы
              </th>
              {/* Удалены поля фильтров, если вы хотите только сортировку */}
              {/* <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Бренд
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Предмет
              </th> */}
              <th
                className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => requestSort('orderscount')}
              >
                Заказы {getSortIndicator('orderscount')}
              </th>
              <th
                className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => requestSort('ad_spend')}
              >
                Реклама {getSortIndicator('ad_spend')}
              </th>
              <th
                className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => requestSort('total_profit')}
              >
                Прибыль {getSortIndicator('total_profit')}
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Действия
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {sortedGroupedSales.map((group) => ( // Используем отсортированные данные
              <tr key={group.imtid}>
                <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                  {group.imtid}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {group.vendorcodes}
                </td>
                {/* Если нужны Бренд и Предмет, верните их сюда */}
                {/* <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {group.brand}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {group.subjectname}
                </td> */}
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {group.orderscount}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {group.ad_spend.toFixed(2)}
                </td>
                <td
                  className={`px-6 py-4 whitespace-nowrap text-sm ${
                    group.total_profit >= 0 ? "text-green-600" : "text-red-600"
                  }`}
                >
                  {group.total_profit.toFixed(2)}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                  <button
                    onClick={() => openModal(group.imtid)}
                    className="text-indigo-600 hover:text-indigo-900"
                  >
                    Подробнее
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {isModalOpen && (
        <div className="fixed inset-0 bg-gray-600 bg-opacity-50 overflow-y-auto h-full w-full flex justify-center items-center">
          <div className="bg-white p-6 rounded-lg shadow-xl w-11/12 max-w-4xl max-h-[90vh] overflow-y-auto">
            <h3 className="text-xl font-bold mb-4">
              Детали по IMT ID: {selectedImt}
            </h3>

            {loading ? (
              <div>Загрузка деталей...</div>
            ) : error ? (
              <div className="text-red-600">{error}</div>
            ) : (
              <>
                <div className="mt-6 p-4 border rounded-lg bg-gray-50 mb-4">
                  <h4 className="font-semibold mb-2">
                    Добавить новую закупочную партию
                  </h4>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                    <label className="block">
                      Артикул (vendorCode):
                      <input
                        type="text"
                        name="vendor_code"
                        className="border p-2 rounded-md w-full mt-1"
                        value={newBatchData.vendor_code}
                        onChange={handleNewBatchChange}
                        list="vendorcodes-list"
                      />
                      <datalist id="vendorcodes-list">
                        {groupDetails.map((item) => (
                          <option key={item.vendorcode} value={item.vendorcode} />
                        ))}
                      </datalist>
                    </label>
                    <label className="block">
                      Закупочная цена (руб):
                      <input
                        type="number"
                        name="purchase_price"
                        className="border p-2 rounded-md w-full mt-1"
                        value={newBatchData.purchase_price}
                        onChange={handleNewBatchChange}
                        min="0"
                        step="0.01"
                      />
                    </label>
                    <label className="block">
                      Количество шт. в партии:
                      <input
                        type="number"
                        name="quantity_bought"
                        className="border p-2 rounded-md w-full mt-1"
                        value={newBatchData.quantity_bought}
                        onChange={handleNewBatchChange}
                        min="1"
                        step="1"
                      />
                    </label>
                    <label className="block">
                      Дата начала действия:
                      <input
                        type="date"
                        name="start_date"
                        className="border p-2 rounded-md w-full mt-1"
                        value={newBatchData.start_date}
                        onChange={handleNewBatchChange}
                      />
                    </label>
                  </div>
                  <button
                    onClick={submitNewBatch}
                    className="mt-4 bg-indigo-600 text-white px-4 py-2 rounded-md hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-opacity-50"
                  >
                    Добавить партию
                  </button>
                </div>

                <button
                  onClick={checkBatches}
                  className="mt-4 mb-4 bg-orange-500 text-white px-4 py-2 rounded-md hover:bg-orange-600 focus:outline-none focus:ring-2 focus:ring-orange-500 focus:ring-opacity-50"
                >
                  Проверить партии сейчас (отправить уведомления)
                </button>

                <h4 className="text-lg font-semibold mb-2">
                  Детали по артикулам для IMT ID {selectedImt}:
                </h4>
                <div className="overflow-x-auto mb-6">
                  <table className="min-w-full divide-y divide-gray-200 border">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Артикул
                        </th>
                        <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Заказы
                        </th>
                        <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Реклама
                        </th>
                        <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Прибыль
                        </th>
                        <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Ср. Цена продажи
                        </th>
                        <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Ср. Себестоимость
                        </th>
                        <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Ср. Закупка
                        </th>
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                      {groupDetails.map((detail) => (
                        <tr key={detail.vendorcode}>
                          <td className="px-4 py-2 whitespace-nowrap text-sm font-medium text-gray-900">
                            {detail.vendorcode}
                          </td>
                          <td className="px-4 py-2 whitespace-nowrap text-sm text-gray-500">
                            {detail.orderscount}
                          </td>
                          <td className="px-4 py-2 whitespace-nowrap text-sm text-gray-500">
                            {detail.ad_spend.toFixed(2)}
                          </td>
                          <td
                            className={`px-4 py-2 whitespace-nowrap text-sm ${
                              detail.total_profit >= 0 ? "text-green-600" : "text-red-600"
                            }`}
                          >
                            {detail.total_profit.toFixed(2)}
                          </td>
                          <td className="px-4 py-2 whitespace-nowrap text-sm text-gray-500">
                            {detail.actual_discounted_price.toFixed(2)}
                          </td>
                          <td className="px-4 py-2 whitespace-nowrap text-sm text-gray-500">
                            {detail.cost_price.toFixed(2)}
                          </td>
                          <td className="px-4 py-2 whitespace-nowrap text-sm text-gray-500">
                            {detail.purchase_price.toFixed(2)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

                <div className="mt-4 p-4 border rounded-lg bg-gray-50">
                  <h4 className="font-semibold mb-2">
                    Изменить себестоимость (другие расходы)
                  </h4>
                  <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-4 gap-3">
                    <label className="block">
                      Артикул для обновления:
                      <input
                        type="text"
                        name="vendorcode"
                        className="border p-2 rounded-md w-full mt-1"
                        value={editData.vendorcode || ""}
                        onChange={handleEditChange}
                        list="vendorcodes-list-edit"
                      />
                      <datalist id="vendorcodes-list-edit">
                        {groupDetails.map((item) => (
                          <option key={item.vendorcode} value={item.vendorcode} />
                        ))}
                      </datalist>
                    </label>
                    <label className="block">
                      Закупочная цена (руб):
                      <input
                        type="number"
                        name="purchase_price"
                        className="border p-2 rounded-md w-full mt-1"
                        value={editData.purchase_price || ""}
                        onChange={handleEditChange}
                        min="0"
                        step="0.01"
                      />
                    </label>
                    <label className="block">
                      Доставка на склад (руб):
                      <input
                        type="number"
                        name="delivery_to_warehouse"
                        className="border p-2 rounded-md w-full mt-1"
                        value={editData.delivery_to_warehouse || ""}
                        onChange={handleEditChange}
                        min="0"
                        step="0.01"
                      />
                    </label>
                    <label className="block">
                      Комиссия WB (руб):
                      <input
                        type="number"
                        name="wb_commission_rub"
                        className="border p-2 rounded-md w-full mt-1"
                        value={editData.wb_commission_rub || ""}
                        onChange={handleEditChange}
                        min="0"
                        step="0.01"
                      />
                    </label>
                    <label className="block">
                      Логистика WB (руб):
                      <input
                        type="number"
                        name="wb_logistics"
                        className="border p-2 rounded-md w-full mt-1"
                        value={editData.wb_logistics || ""}
                        onChange={handleEditChange}
                        min="0"
                        step="0.01"
                      />
                    </label>
                    <label className="block">
                      Налог (руб):
                      <input
                        type="number"
                        name="tax_rub"
                        className="border p-2 rounded-md w-full mt-1"
                        value={editData.tax_rub || ""}
                        onChange={handleEditChange}
                        min="0"
                        step="0.01"
                      />
                    </label>
                    <label className="block">
                      Упаковка (руб):
                      <input
                        type="number"
                        name="packaging"
                        className="border p-2 rounded-md w-full mt-1"
                        value={editData.packaging || ""}
                        onChange={handleEditChange}
                        min="0"
                        step="0.01"
                      />
                    </label>
                    <label className="block">
                      Топливо (руб):
                      <input
                        type="number"
                        name="fuel"
                        className="border p-2 rounded-md w-full mt-1"
                        value={editData.fuel || ""}
                        onChange={handleEditChange}
                        min="0"
                        step="0.01"
                      />
                    </label>
                    <label className="block">
                      Подарок (руб):
                      <input
                        type="number"
                        name="gift"
                        className="border p-2 rounded-md w-full mt-1"
                        value={editData.gift || ""}
                        onChange={handleEditChange}
                        min="0"
                        step="0.01"
                      />
                    </label>
                    <label className="block">
                      Процент брака (%):
                      <input
                        type="number"
                        name="defect_percent"
                        className="border p-2 rounded-md w-full mt-1"
                        value={editData.defect_percent || ""}
                        onChange={handleEditChange}
                        min="0"
                        step="0.01"
                      />
                    </label>
                  </div>
                  <button
                    onClick={submitCostUpdate}
                    className="mt-4 bg-green-500 text-white px-4 py-2 rounded-md hover:bg-green-600 focus:outline-none focus:ring-2 focus:ring-green-500 focus:ring-opacity-50"
                  >
                    Обновить расходы для артикула
                  </button>
                </div>

                <h4 className="text-lg font-semibold mb-2 mt-6">
                  Ежедневные продажи для IMT ID {selectedImt}:
                </h4>
                <div className="overflow-x-auto">
                  <table className="min-w-full divide-y divide-gray-200 border">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Дата
                        </th>
                        <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Заказы
                        </th>
                        <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Реклама
                        </th>
                        <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Прибыль
                        </th>
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                      {dailySales.map((day) => (
                        <tr key={day.date}>
                          <td className="px-4 py-2 whitespace-nowrap text-sm font-medium text-gray-900">
                            {day.date}
                          </td>
                          <td className="px-4 py-2 whitespace-nowrap text-sm text-gray-500">
                            {day.orderscount}
                          </td>
                          <td className="px-4 py-2 whitespace-nowrap text-sm text-gray-500">
                            {day.ad_spend.toFixed(2)}
                          </td>
                          <td
                            className={`px-4 py-2 whitespace-nowrap text-sm ${
                              day.total_profit >= 0 ? "text-green-600" : "text-red-600"
                            }`}
                          >
                            {day.total_profit.toFixed(2)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </>
            )}

            <div className="mt-4 flex justify-end">
              <button
                onClick={closeModal}
                className="px-4 py-2 bg-gray-300 text-gray-800 rounded-md hover:bg-gray-400 focus:outline-none focus:ring-2 focus:ring-gray-500 focus:ring-opacity-50"
              >
                Закрыть
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default Dashboard;