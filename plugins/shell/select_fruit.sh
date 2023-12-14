FRUIT=$1 # $1 : 입력 변수 중 첫번째 변수
if [ $FRUIT == APPLE ]; then
	echo "You selected Apple!"
elif [ $FRUIT == ORANGE ]; then
	echo "You selected Orange!"
elif [ $FRUIT == GRAPE ]; then
	echo "You selected Grape!"
else
	echo "You selected other Fruit!"
fi

