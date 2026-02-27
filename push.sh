# To run type ./push.sh
git add .
git commit -m "Auto update: $(date '+%Y-%m-%d %H:%M:%S')"
git push
echo "Repository uploaded."