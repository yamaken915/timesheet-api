// 📥 CSVダウンロード
function downloadCSV() {
  window.location.href = "/holidays/download";
}

// 📤 CSVアップロード
async function uploadCSV() {
  const input = document.getElementById("csvInput");
  const file = input.files[0];
  const resultMsg = document.getElementById("resultMsg");
  resultMsg.textContent = "";
  resultMsg.style.color = "black";

  if (!file) {
    resultMsg.textContent = "CSVファイルを選択してください。";
    resultMsg.style.color = "red";
    return;
  }

  const formData = new FormData();
  formData.append("file", file);

  try {
    const res = await fetch("/holidays/upload", {
      method: "POST",
      body: formData
    });

    const msg = await res.text();
    resultMsg.textContent = msg;

    if (!res.ok) {
      resultMsg.style.color = "red";
    } else {
      resultMsg.style.color = "green";
    }
  } catch (err) {
    resultMsg.textContent = "アップロードに失敗しました。";
    resultMsg.style.color = "red";
  }
}
